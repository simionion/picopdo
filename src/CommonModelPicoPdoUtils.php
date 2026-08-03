<?php
declare(strict_types=1);

namespace Lodur\PicoPdo;



use InvalidArgumentException;

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
     * Build a temp-table batch UPDATE plan, or `[]` when the payload must stay on CASE/WHEN.
     *
     * Per-row JOIN keys must be plain column equalities. Conditions identical on every where-row
     * are peeled off and applied once on alias `t` (shared `n > ?` / named placeholder / equality).
     * Bindings feed only that shared clause: a named map, or a per-row list whose values for those
     * placeholders are identical. Empty plan = fall back — never silently wrong.
     *
     * @param list<DataMap> $data
     * @param list<DataMap|string> $where
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings
     * @return array{
     *     target: string,
     *     rowWhere: list<DataMap>,
     *     common: array<string|int, mixed>,
     *     bindings: BindingsMap
     * }|array{}
     */
    public static function buildTempTableUpdate(
        string $table,
        array $data,
        array $where,
        mixed $bindings,
        string|null $sqlTail
    ): array {
        $target = self::quoteIdentifier($table);

        if (
            count($data) < self::TEMP_UPDATE_MIN_ROWS
            || trim((string) $sqlTail) !== ''
            || $target === null
            || array_filter(
                $where,
                static fn(mixed $condition): bool => !is_array($condition)
            )
        ) {
            return [];
        }

        [$common, $rowWhere] = self::splitCommonWhereRows($where);

        if ($rowWhere === []) {
            return [];
        }

        $cols = array_keys($data[0]);
        $keys = array_keys($rowWhere[0]);

        if (
            $keys === []
            || $cols === []
            || array_intersect($keys, $cols) !== []
            || array_filter(
                [...$keys, ...$cols],
                static fn(mixed $identifier): bool =>
                    !is_string($identifier)
                    || self::quoteIdentifier($identifier) === null
            )
        ) {
            return [];
        }

        // Qualified against the columns the staging table will carry, so the shared clause can
        // never leave an ambiguous bare reference behind.
        $qualifiedCommon = self::qualifyWhereForAlias($common, 't', [...$keys, ...$cols]);

        if ($common !== [] && $qualifiedCommon === []) {
            return [];
        }

        $sharedBindings = self::sharedRowBindings($common, $bindings, count($data));
        if ($sharedBindings === null) {
            return [];
        }

        $seenConditions = [];

        foreach ($data as $i => $row) {
            $condition = $rowWhere[$i];

            if (
                array_keys($row) !== $cols
                || array_keys($condition) !== $keys
                || array_filter(
                    [...$row, ...$condition],
                    static fn(mixed $value): bool =>
                        $value !== null && !is_scalar($value)
                )
            ) {
                return [];
            }

            // CASE/WHEN has deterministic first-match behaviour for duplicate conditions; a
            // staging-table JOIN does not. Keep duplicates on the safe CASE/WHEN path.
            $conditionKey = serialize($condition);

            if (isset($seenConditions[$conditionKey])) {
                return [];
            }

            $seenConditions[$conditionKey] = true;
        }

        return [
            'target' => $target,
            'rowWhere' => $rowWhere,
            'common' => $qualifiedCommon,
            'bindings' => $sharedBindings,
        ];
    }

    /**
     * Prefix the leading column reference of every WHERE entry with `$alias.` for JOIN UPDATEs.
     *
     * Plain `col => val`, keyed `col … ?` / `col … :name`, and raw fragments that start with a
     * column identifier are rewritten. Returns an empty array when any entry cannot be prefixed
     * safely — callers treat that as “fall back” when the input was non-empty. An empty input
     * stays empty (nothing to qualify).
     *
     * Only the *leading* identifier is prefixed, so a fragment naming several columns keeps its
     * later references bare. Bare is fine while the name exists on one side of the JOIN only, and
     * `$staged` — the columns the staging table carries — is what makes that checkable: a later
     * reference to one of them is ambiguous to the server (error 1052), so the whole entry is
     * refused instead. Pass `[]` to skip the check when nothing is staged alongside.
     *
     * @param array<string|int, mixed> $where
     * @param list<string> $staged Columns present on the other side of the JOIN
     * @return array<string|int, mixed>
     */
    public static function qualifyWhereForAlias(array $where, string $alias, array $staged = []): array
    {
        $qualified = [];

        foreach ($where as $key => $value) {
            $fragment = is_int($key) ? $value : $key;
            $prefixed = is_string($fragment) ? self::prefixLeadingIdentifier($fragment, $alias) : null;

            if ($prefixed === null || self::hasBareColumnReference($prefixed, $staged)) {
                return [];
            }

            if (is_int($key)) {
                $qualified[$key] = $prefixed;
            } else {
                $qualified[$prefixed] = $value;
            }
        }

        return $qualified;
    }

    /**
     * Whether a SQL fragment names one of `$columns` without a table qualifier.
     *
     * Deliberately blunt: every bare word is a candidate, so a column name appearing inside a
     * string literal counts too. `:name` placeholders and already-qualified `x.name` references
     * do not — they cannot be ambiguous. Over-reporting only costs the caller its fast path.
     *
     * @param list<string> $columns
     */
    public static function hasBareColumnReference(string $sql, array $columns): bool
    {
        if ($columns === [] || preg_match_all('/(?<![:.\w])[A-Za-z_][A-Za-z0-9_]*/', $sql, $matches) < 1) {
            return false;
        }

        $columns = array_map(strtolower(...), $columns);

        foreach ($matches[0] as $word) {
            if (in_array(strtolower($word), $columns, true)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Resolve bindings for the shared (peeled) WHERE clause of a temp-table UPDATE.
     *
     * The same rule as the WHERE peel, applied to bindings: only values identical on every row
     * can feed the shared clause. A per-row list is reduced with {@see splitCommonWhereRows()};
     * a plain named map is used as-is. Returns exactly the placeholders `$common` references —
     * `:name` binds plus the leading positional values its raw fragments consume — or `null` when
     * one of them is missing or varies per row (caller falls back).
     *
     * @param array<string|int, mixed> $common
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings
     * @return BindingsMap|null
     */
    public static function sharedRowBindings(array $common, mixed $bindings, int $rowCount): array|null
    {
        $names = self::namedPlaceholdersInWhere($common);
        $positional = self::positionalPlaceholdersInWhere($common);
        $bindings = self::normalizePositionalBindings($bindings) ?? [];

        if (!is_array($bindings)) {
            // A scalar can only feed the single-column shorthand, never a batch clause.
            return null;
        }

        if ($bindings !== [] && self::hasOnlyIntKeys($bindings)) {
            if (
                count($bindings) !== $rowCount
                || array_filter($bindings, static fn(mixed $row): bool => !is_array($row))
            ) {
                return null;
            }
            [$bindings] = self::splitCommonWhereRows($bindings);
        }

        $shared = array_intersect_key($bindings, array_flip($names));

        if (count($shared) !== count($names)) {
            return null;
        }

        // Raw fragments draw on the positional list in order, so the shared clause needs indexes
        // 0..n-1 present — and no more than it consumes, which buildSqlClause() rejects.
        for ($i = 0; $i < $positional; $i++) {
            if (!array_key_exists($i, $bindings)) {
                return null;
            }
            $shared[$i] = $bindings[$i];
        }

        return $shared;
    }

    /**
     * Rewrite user-supplied `:name` placeholders under `$prefix` so they cannot collide with
     * other clauses. Already-prefixed names are left untouched.
     *
     * With `$bound = null` every user name is renamed (shared-clause isolation). With a map of
     * already-bound params, only genuine conflicts are renamed: a name re-bound to the same
     * value stays shared and is bound once for the whole statement — the bindings analogue of
     * the identical-condition peel in {@see buildTempTableUpdate()}.
     *
     * @param BindingsMap $params
     * @param BindingsMap|null $bound Params accumulated from earlier batch rows
     * @return array{0: string, 1: BindingsMap}
     */
    public static function isolateNamedPlaceholders(
        string $sql,
        array $params,
        string $prefix,
        array|null $bound = null
    ): array {
        $out = [];

        foreach ($params as $name => $value) {
            $bare = is_string($name) && str_starts_with($name, ':') ? substr($name, 1) : null;
            $isConflict = $bound === null
                || (array_key_exists($name, $bound) && $bound[$name] !== $value);

            if ($bare === null || str_starts_with($bare, $prefix) || !$isConflict) {
                $out[$name] = $value;
                continue;
            }

            $isolated = ':' . $prefix . $bare;
            $sql = preg_replace('/' . preg_quote($name, '/') . '\b/', $isolated, $sql) ?? $sql;
            $out[$isolated] = $value;
        }

        return [$sql, $out];
    }

    /**
     * @param array<string|int, mixed> $where
     * @return list<string> Placeholder names including the leading colon
     */
    public static function namedPlaceholdersInWhere(array $where): array
    {
        $sql = implode(' ', array_filter(
            [...array_keys($where), ...array_values($where)],
            is_string(...)
        ));
        preg_match_all('/:[A-Za-z_][A-Za-z0-9_]*/', $sql, $matches);

        return array_values(array_unique($matches[0]));
    }

    /**
     * How many positional `?` a WHERE map consumes from the bindings list.
     *
     * Only numeric-keyed raw fragments draw on that list; a keyed `col > ?` entry carries its own
     * value and is not counted — the same split {@see CommonModelPicoPdoTrait::buildSqlClause()}
     * makes when it compiles the clause.
     *
     * @param array<string|int, mixed> $where
     */
    public static function positionalPlaceholdersInWhere(array $where): int
    {
        $count = 0;

        foreach ($where as $key => $fragment) {
            if (is_numeric($key) && is_string($fragment)) {
                $count += substr_count($fragment, '?');
            }
        }

        return $count;
    }

    /**
     * Prefix the leading column identifier with `$alias.`, or null when the fragment does not
     * start with one (e.g. `(col …)`) — the caller then falls back rather than guess a prefix.
     *
     * An already-qualified `tbl.col` is refused too: the alias replaces the table name in the
     * JOIN, so neither prefixing it (`t.tbl.col`) nor leaving it be resolves to anything.
     */
    public static function prefixLeadingIdentifier(string $sql, string $alias): string|null
    {
        return preg_match('/^(\s*)`?([A-Za-z_][A-Za-z0-9_]*)`?(.*)$/s', $sql, $matches) === 1
        && !str_starts_with($matches[3], '.')
            ? "{$matches[1]}{$alias}.{$matches[2]}{$matches[3]}"
            : null;
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

    /**
     * Whether insert rows require more than one per-column-shape statement.
     *
     * @param array<int|string, mixed> $rows
     */
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

    /**
     * Whether every key is an integer; unlike array_is_list(), gaps are accepted.
     *
     * @param array<int|string, mixed> $value
     */
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
     *
     * @phpstan-assert-if-true array<int, array<string|int, mixed>> $value
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
            throw new InvalidArgumentException("Invalid table name: {$table}");
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
            throw new InvalidArgumentException("{$operation} requires a WHERE condition");
        }

        return preg_match('/^WHERE\b/i', $whereClause) === 1
            ? $whereClause
            : 'WHERE ' . $whereClause;
    }

    /**
     * Split a raw `column = expression` assignment and reject malformed entries early.
     *
     * @return array{0: string, 1: string} Column and expression, both non-empty.
     */
    public static function splitAssignment(string $assignment): array
    {
        $parts = explode('=', $assignment, 2);
        if (count($parts) !== 2) {
            throw new InvalidArgumentException(
                "Raw SQL assignment must contain '=': {$assignment}"
            );
        }

        $column = trim($parts[0]);
        $expression = trim($parts[1]);
        if ($column === '' || $expression === '') {
            throw new InvalidArgumentException(
                "Raw SQL assignment is incomplete: {$assignment}"
            );
        }

        return [$column, $expression];
    }

    /**
     * Split batch WHERE rows into conditions shared by every row and conditions
     * that vary per row.
     *
     * @param list<array<string|int, mixed>> $whereRows
     * @return array{
     *     0: array<string|int, mixed>,
     *     1: list<array<string|int, mixed>>
     * }
     */
    public static function splitCommonWhereRows(array $whereRows): array
    {
        $common = $whereRows[0] ?? [];

        foreach (array_slice($whereRows, 1) as $row) {
            $common = array_filter(
                $common,
                static fn(
                    mixed $value,
                    int|string $key
                ): bool => array_key_exists($key, $row)
                    && $row[$key] === $value,
                ARRAY_FILTER_USE_BOTH
            );
        }

        return [
            $common,
            array_map(
                static fn(array $row): array =>
                array_diff_key($row, $common),
                $whereRows
            ),
        ];
    }
}