<?php
declare(strict_types=1);

namespace Lodur\PicoPdo;



/**
 * Stateless helpers for {@see CommonModelPicoPdoTrait}.
 *
 * A trait's private methods are copied into every class that uses it, and a class defining the
 * same name silently wins — no error, no warning — so a model that happens to declare, say,
 * `chunkList()` would quietly break the trait from the inside. Everything here that needs no
 * connection lives outside the trait for that reason; only the methods that genuinely require
 * `$this` (the PDO handle, or the trait's own SQL builders) remain in it.
 *
 * These are also the parts that are testable without a database.
 *
 * @phpstan-type BindingsMap array<string|int, mixed>
 * @phpstan-type DataMap array<string|int, mixed>
 */
final class CommonModelPicoPdoUtils
{
    /**
     * Largest statement the server will accept, in bytes (`max_allowed_packet`).
     *
     * Overshooting is not recoverable: the server answers 1153 and drops the connection
     * (`2006 server has gone away`), so an oversized batch cannot be split and retried
     * afterwards — it has to be split before it is sent.
     */
    public const int MAX_ALLOWED_PACKET = 16777216;

    /**
     * Share of {@see MAX_ALLOWED_PACKET} a batch may fill, measured with `serialize()`.
     *
     * `serialize()` only approximates the statement, so the ratio has to absorb the error. The
     * binding case is a batch UPDATE: its CASE/WHEN scaffolding costs ~24 bytes per row *per
     * column* (`WHEN (a = 5) THEN 2`), so the statement runs larger the narrower the columns
     * are. Measured across 1–30 columns the statement peaks at 1.37x its serialized size and
     * then flattens, which caps the safe fill at 0.73; a multi-row INSERT runs *under* its
     * serialized size (0.57x), so it is never the constraint.
     *
     * 0.5 keeps the worst measured shape near 69% of the packet, leaving room for shapes not
     * measured. Nothing ordinary is affected: real batches are KB-scale, so the guard only ever
     * fires on payloads that would otherwise have dropped the connection.
     */
    public const float PACKET_FILL_RATIO = 0.5;

    /**
     * Row count from which a batch UPDATE stages through a temporary table.
     *
     * The CASE/WHEN form encodes every row into the statement text, and the server has no index
     * into a CASE: it walks the WHEN list for each row it touches, so cost grows with rows x
     * branches. Staging into a temporary table and joining makes it one indexed lookup per row.
     *
     * Measured locally: 100 rows 1.6 ms vs 2.3 ms (CASE/WHEN still ahead — creating and dropping
     * the table is not free), 1000 rows 29 ms vs 7 ms, 10000 rows 3305 ms vs 81 ms. The crossover
     * sits near 200 rows, so below this the simpler single statement is kept.
     */
    public const int TEMP_UPDATE_MIN_ROWS = 200;

    /** Add LIMIT 1 unless the caller already supplied a LIMIT in the SQL tail. */
    public static function appendLimitOne(string|null $sqlTail): string
    {
        $sqlTail = trim((string)$sqlTail);
        return self::hasLimit($sqlTail) ? $sqlTail : trim("{$sqlTail} LIMIT 1");
    }

    /**
     * Compiles insert rows into per-shape VALUES batches in a single pass.
     *
     * Each row is parsed exactly once: string keys become bound `:row_{i}_col` placeholders, numeric keys
     * carry raw SQL assignments like `'created_at = NOW()'` whose expression is inlined verbatim. Rows are
     * then grouped by their column list, so {@see insert()} can execute one multi-row statement per shape.
     * The row index `{i}` is global across the whole payload, keeping placeholders unique between batches.
     *
     * ```
     * buildInsertBatches([
     *     ['name' => 'Ion', 'created_at = NOW()'],
     *     ['name' => 'Ani', 'created_at = NOW()'],
     * ]);
     * // Returns:
     * // [
     * //     'name|created_at' => [
     * //         'name, created_at',                                    // column list
     * //         ['(:row_0_name, NOW())', '(:row_1_name, NOW())'],      // one value tuple per row
     * //         [':row_0_name' => 'Ion', ':row_1_name' => 'Ani'],      // bound params
     * //     ],
     * // ]
     * ```
     *
     * @param list<DataMap> $rows Insert rows (a single-row insert is passed as a one-element list).
     * @return array<string, array{0: string, 1: list<string>, 2: BindingsMap}> Batches keyed by column
     * signature: [column list SQL, row value tuples, bound params].
     */
    public static function buildInsertBatches(array $rows): array
    {
        $batches = [];
        foreach ($rows as $i => $row) {
            if ($row === []) {
                // No columns to set: skip instead of emitting `INSERT INTO t () VALUES ()`, which
                // would insert an all-defaults row.
                continue;
            }
            $columns = $values = $params = [];
            foreach ($row as $key => $value) {
                if (is_numeric($key)) {
                    // Raw SQL assignment: split 'created_at = NOW()' into column + inlined expression.
                    [$column, $expression] = self::splitAssignment((string)$value);
                } else {
                    // Key-value pair: bind the value under a per-row unique placeholder.
                    [$column, $expression] = [$key, ":row_{$i}_{$key}"];
                    $params[$expression] = $value;
                }
                $columns[] = $column;
                $values[] = $expression;
            }
            $shape = implode('|', $columns);
            $batches[$shape][0] ??= implode(', ', $columns);
            $batches[$shape][1][] = '(' . implode(', ', $values) . ')';
            $batches[$shape][2] ??= [];   // a row of only raw SQL binds nothing

            foreach ($params as $placeholder => $value) {
                $batches[$shape][2][$placeholder] = $value;
            }
        }
        return $batches;
    }

    /**
     * Whether a batch update can be staged through a temporary table and applied with a JOIN.
     *
     * A JOIN can only express "match these columns by equality", so everything the CASE/WHEN
     * form supports but a join cannot — raw SQL fragments, `?` keys, per-row bindings, a
     * trailing LIMIT, rows with differing shapes — falls back. The check is deliberately strict:
     * the fallback is merely slower, while a wrong join silently updates the wrong rows.
     *
     * @param list<DataMap> $data
     * @param list<DataMap|string> $where
     */
    public static function canUpdateViaTempTable(string $table, array $data, array $where, mixed $bindings, string|null $sqlTail): bool
    {
        if (count($data) < self::TEMP_UPDATE_MIN_ROWS || $bindings !== null || trim((string)$sqlTail) !== '' || self::quoteIdentifier($table) === null) {
            return false;
        }

        $keys = array_keys((array)($where[0] ?? []));
        $cols = array_keys((array)($data[0] ?? []));

        if ($keys === [] || $cols === [] || array_intersect($keys, $cols) !== []) {
            return false;   // nothing to join on, nothing to set, or the join key is being rewritten
        }

        foreach ([...$keys, ...$cols] as $identifier) {
            if (self::quoteIdentifier((string)$identifier) === null) {
                return false;   // raw fragment, `col = ?` key, or a numeric (raw SQL) key
            }
        }

        $seenConditions = [];

        foreach ($data as $i => $row) {
            $condition = $where[$i] ?? null;

            if (!is_array($row) || !is_array($condition)
                || array_keys($row) !== $cols || array_keys($condition) !== $keys) {
                return false;   // rows must share one shape to become table columns
            }

            foreach ([...$row, ...$condition] as $value) {
                if ($value !== null && !is_scalar($value)) {
                    return false;
                }
            }

            // CASE/WHEN has deterministic first-match behaviour for duplicate conditions; a
            // staging-table JOIN does not. Keep duplicates on the safe CASE/WHEN path.
            $conditionKey = serialize($condition);
            if (isset($seenConditions[$conditionKey])) {
                return false;
            }
            $seenConditions[$conditionKey] = true;
        }

        return true;
    }

    /**
     * Splits rows into chunks whose statement fits the packet, halving while a chunk is too big.
     *
     * A single row is never split — it is the smallest unit a statement can carry, so an
     * oversized row is returned as its own chunk and left to fail loudly rather than be
     * silently truncated.
     *
     * @template TRow
     * @param list<TRow> $rows
     * @param int $fixedBytes Serialized payload repeated in every generated statement
     * @return list<list<TRow>> Never empty; a batch that already fits is returned unchanged.
     */
    public static function chunkForPacket(array $rows, int $fixedBytes = 0): array
    {
        // A single row cannot be split further, so skip sizing it — this is the hot path for
        // every non-batch write.
        if (count($rows) < 2) {
            return [$rows];
        }

        // serialize() over json_encode(): it never fails on binary column values, and its
        // per-value overhead (`i:2;`, `s:5:"..."`) tracks the SQL scaffolding far more closely.
        $size = strlen(serialize($rows)) + $fixedBytes;

        if ($size < self::MAX_ALLOWED_PACKET * self::PACKET_FILL_RATIO) {
            return [$rows];
        }

        $half = (int)ceil(count($rows) / 2);

        return array_merge(
            self::chunkForPacket(array_slice($rows, 0, $half), $fixedBytes),
            self::chunkForPacket(array_slice($rows, $half), $fixedBytes),
        );
    }

    /**
     * Slices one chunk out of a parallel list.
     *
     * A batched write carries several inputs that run parallel per row — the SET maps, the WHERE
     * rows and a per-row bindings list — so a chunk has to take the same slice of each.
     * {@see buildUpdateSqlParts()} and {@see buildDeleteSqlParts()} read them positionally
     * (`$bindings[$i]`), and chunks are uneven because {@see chunkForPacket()} halves, so the
     * slice has to be taken by explicit offset and length rather than a fixed stride.
     *
     * A value that is not a list is shared by every row — a single bindings map, or a WHERE that
     * applies throughout — and is passed through untouched.
     *
     * @param int|string|array<mixed>|null $value
     * @return int|string|array<mixed>|null
     */
    public static function chunkList(int|string|array|null $value, int $offset, int $length): int|string|array|null
    {
        return is_array($value) && array_is_list($value)
            ? array_slice($value, $offset, $length)
            : $value;
    }

    /** Whether a trailing SQL fragment contains a LIMIT clause. */
    public static function hasLimit(string|null $sqlTail): bool
    {
        return preg_match('/\bLIMIT\b/i', (string)$sqlTail) === 1;
    }

    /** Whether insert rows require more than one per-column-shape statement. */
    public static function hasMultipleInsertShapes(array $rows): bool
    {
        $firstShape = null;

        foreach ($rows as $row) {
            if ($row === []) {
                continue;
            }

            $columns = [];
            foreach ($row as $key => $value) {
                $columns[] = is_numeric($key)
                    ? self::splitAssignment((string)$value)[0]
                    : (string)$key;
            }

            $shape = implode('|', $columns);
            if ($firstShape === null) {
                $firstShape = $shape;
            } elseif ($shape !== $firstShape) {
                return true;
            }
        }

        return false;
    }

    /** Whether every key is an integer; unlike array_is_list(), gaps are accepted. */
    public static function hasOnlyIntKeys(array $value): bool
    {
        foreach ($value as $key => $_) {
            if (!is_int($key)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Whether the value is a set of rows: integer-keyed, every entry an array.
     *
     * Used instead of `array_is_list()`, which additionally demands the keys be 0..n-1 with no
     * gaps. Callers routinely build batches with `array_diff()`, `array_filter()` or similar,
     * all of which preserve keys — a perfectly good batch then failed the list test and fell
     * through to the single-row path, producing `INSERT INTO t (Array) VALUES ()` rather than an
     * error. Column names are strings, so a single row never satisfies this test, not even one
     * whose value is an array (`['ids' => [1, 2]]`).
     */
    public static function isRowSet(mixed $value): bool
    {
        if (!is_array($value) || $value === [] || !self::hasOnlyIntKeys($value)) {
            return false;
        }

        foreach ($value as $row) {
            if (!is_array($row)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Re-indexes a per-row bindings list, leaving a shared bindings map untouched.
     *
     * {@see buildUpdateSqlParts()} and {@see buildDeleteSqlParts()} read `$bindings[$i]` by
     * position, so a gapped list would pair rows with the wrong values. A map keyed by
     * placeholder name applies to every row and must survive as-is.
     *
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings
     * @return int|string|BindingsMap|list<int|string|BindingsMap>|null
     */
    public static function normalizePositionalBindings(int|string|array|null $bindings): int|string|array|null
    {
        if ($bindings === []) {
            return null;
        }

        return is_array($bindings) && self::hasOnlyIntKeys($bindings)
            ? array_values($bindings)
            : $bindings;
    }

    /**
     * Validate and normalize a plain table name used by DESCRIBE helpers.
     *
     * Silently stripping characters can turn an invalid name into a different valid table, so
     * reject anything except a single unqualified SQL identifier. Surrounding backticks are
     * accepted for convenience and removed before validation.
     *
     * @param string $table The table name to normalize
     * @return string Valid plain table name without backticks
     */
    public static function normalizeTableName(string $table): string
    {
        $table = trim($table, "` \t\n\r\0\x0B");
        if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $table) !== 1) {
            throw new \InvalidArgumentException("Invalid table name: {$table}");
        }

        return $table;
    }

    /**
     * Backtick-quotes a table or column name, or null when it is not a plain identifier.
     *
     * Used to decide whether the temp-table path is even possible: anything carrying SQL —
     * `created_at = NOW()`, `id IN (?)`, a numeric (raw) key — is not an identifier and sends
     * the caller back to the CASE/WHEN form.
     */
    public static function quoteIdentifier(int|string $name): string|null
    {
        return is_string($name) && preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $name) === 1 ? "`{$name}`" : null;
    }

    /** Prefix a compiled condition with WHERE and reject accidental full-table writes. */
    public static function requireWhereClause(string $whereClause, string $operation): string
    {
        $whereClause = trim($whereClause);
        if ($whereClause === '') {
            throw new \InvalidArgumentException("{$operation} requires a WHERE condition");
        }

        return preg_match('/^WHERE\b/i', $whereClause) === 1
            ? $whereClause
            : 'WHERE ' . $whereClause;
    }

    /** Split a raw `column = expression` assignment and reject malformed entries early. */
    public static function splitAssignment(string $assignment): array
    {
        $parts = explode('=', $assignment, 2);
        if (count($parts) !== 2) {
            throw new \InvalidArgumentException(
                "Raw SQL assignment must contain '=': {$assignment}"
            );
        }

        $column = trim($parts[0]);
        $expression = trim($parts[1]);
        if ($column === '' || $expression === '') {
            throw new \InvalidArgumentException(
                "Raw SQL assignment is incomplete: {$assignment}"
            );
        }

        return [$column, $expression];
    }
}