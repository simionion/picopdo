<?php
declare(strict_types=1);

namespace Lodur\PicoPdo;

use InvalidArgumentException;
use PDO;
use PDOException;
use PDOStatement;
use RuntimeException;
use Throwable;



/**
 * Trait CommonModelPicoPdoTrait
 *
 * Provides a lightweight, reusable set of methods for interacting with a database using PDO.
 *
 * Designed to be used within model classes, this trait offers common CRUD operations and
 * utility methods to simplify and standardize database access logic.
 *
 * ## Usage:
 * - Intended to be included in models that require direct database interaction.
 * - Requires a `$db` property of type PDO to be defined in the consuming class. Legacy models using `$pdo` remain supported through {@see pdo()}.
 * - Provides protected helper methods for common database tasks.
 *
 * ## Features:
 * - Secure, parameterized queries via `prepExec()`, with array-expansion for `IN` clauses.
 * - Simplified methods for SELECT, INSERT, UPDATE, DELETE, and EXISTS operations.
 * - Automatically binds parameters and handles execution errors.
 * - Reduces duplication and boilerplate in model implementations.
 * - **Mixed placeholders** — `:named` and `?` can appear in the same WHERE/SET clause; pass named keys
 *   and positional values in one bindings array (e.g. `[':status' => 'active', 5]`).
 *
 * ## Common arguments
 *
 * Most CRUD helpers share the same parameters. Each accepts several input shapes; the trait normalizes
 * them into parameterized SQL before execution.
 *
 * ### `$table` (string)
 * Table or `FROM` fragment, passed through as-is into the generated SQL.
 *
 * Simple name:
 * ```
 * 'users'
 * 'order_items'
 * ```
 *
 * SELECT / EXISTS — simple JOIN (qualify `$columns` and `$where` with table aliases):
 * ```
 * $db->selectAll(
 *     'users U INNER JOIN profiles P ON P.user_id = U.id',
 *     ['U.name', 'P.bio'],
 *     ['U.status' => 'active']
 * );
 * ```
 *
 * INSERT / UPDATE / DELETE expect a single target table name; use `prepExec()` for more complex statements.
 *
 * ### `$columns` (SELECT only — `list<string>|string|null`, default `'*'`)
 * - `'id, name, email'` — comma-separated column list
 * - `['id', 'name', 'email']` — array of column names
 * - `null` or omitted — selects `*`
 *
 * ### `$data` (`DataMap` or `list<DataMap>`)
 * Column values and expressions. Used as INSERT row(s) or UPDATE `SET` clause(s).
 *
 * **Single map** — one row / one update:
 * - `'name' => 'John'` — `name = :prefix_name`
 * - `'views = views + 1'` — numeric key: raw SQL pasted into the clause (same as insert)
 * - `'last_login = ?' => $date` — key contains `?`: bound expression
 * - `'created_at = NOW()'` — numeric key: raw SQL with no binding
 *
 * **List of maps** — multi-row INSERT, or batch UPDATE when `$where` is also a list (one map per row).
 * Rows with different column sets are grouped and inserted in separate statements.
 *
 * ### `$where` (`WhereInput` — `string|array|null`)
 * Row filter. Omitted or empty on UPDATE/DELETE throws `InvalidArgumentException`, guarding full-table writes.
 *
 * **Shorthand** — column name + scalar `$bindings`:
 * ```
 * $where = 'id', $bindings = 1        // id = :where_id
 * ```
 *
 * **Condition string** — raw SQL with optional placeholders:
 * ```
 * 'email = ? AND status = ?'
 * 'status = :status AND id > ?'       // mixed `:named` and `?` supported
 * 'id IN (:ids)'                      // array binding expands IN list
 * 'id IN (?)'                         // positional IN: pass `[[1, 2, 3]]`
 * ```
 *
 * **Associative array** — AND-joined conditions (same rules as `$data` keys):
 * ```
 * ['id' => 1, 'status' => 'active']                    // equality
 * ['email_verified != 0', 'created_at > :date']        // raw SQL fragments
 * ['created_at > ?' => $date]                          // `?` key binds value
 * ```
 * Extra bindings for raw fragments can live in `$bindings` when not inline.
 *
 * **List** (batch UPDATE only) — one condition per `$data` row, same index order.
 *
 * ### `$bindings` (`int|string|BindingsMap|list<...>|null`)
 * Values for placeholders in `$where` (or in raw `$data` / `$where` entries).
 *
 * - Scalar — used with column-name shorthand: `'id', 1`
 * - Positional list — for `?` placeholders: `['a@example.com', 'active']`
 * - Named map — for `:placeholder` keys: `[':status' => 'active', ':ids' => [1, 2, 3]]`.
 *   On batch operations the map is shared: each name is bound once for the whole statement.
 * - Per-row list (batch UPDATE/DELETE) — `[$bindingsRow0, $bindingsRow1, …]` aligned with `$data` /
 *   `$where`. A `:name` reused across rows is isolated per row when the values differ; identical
 *   values collapse to one shared bind.
 *
 * ### `$sqlTail` (`string|null`)
 * Raw SQL suffix appended to the generated statement (e.g. `'ORDER BY id'`, `'ORDER BY id LIMIT 10'`).
 * On batch UPDATE, a trailing `LIMIT` caps one generated statement; packet-split batches reject `LIMIT`.
 *
 * ### `$options` (INSERT only)
 * `['mode' => 'INSERT'|'REPLACE'|'INSERT IGNORE', 'meta' => bool, 'onDuplicateKeyUpdate' => DataMap]`
 *
 * @update Automatic conversion of `?` placeholders to named placeholders
 * All `?` placeholders in WHERE clauses and SQL clauses are automatically converted to named placeholders
 * (e.g., `:where_0`, `:where_1`, `:set_0`, etc.) for consistency and better support (ex: usage of IN(?)  [[1,2,3]]).
 *
 * @author Ion Simion
 * @repository https://github.com/simionion/picopdo
 *
 * @phpstan-type BindingsMap array<string|int, mixed>
 * @phpstan-type DataMap array<string|int, mixed>
 * @phpstan-type WhereInput string|array<string|int, mixed>|null
 */
trait CommonModelPicoPdoTrait
{
    protected PDO $pdo;

    /**
     * The connection every trait method runs on: `$this->db`, else a legacy `$this->pdo`.
     *
     * Protected so a class holding its handle elsewhere can override it.
     */
    protected function pdo(): PDO
    {
        if (isset($this->pdo)) {
            return $this->pdo;
        }

        $vars = get_object_vars($this);
        foreach (['db', 'oDB'] as $property) {
            if (($vars[$property] ?? null) instanceof PDO) {
                return $vars[$property];
            }
        }

        throw new RuntimeException('No valid PDO connection found');
    }

    /**
     * Combines `prepare` & `execute` in a single function and returns the PDO statement.
     *
     *  - When a parameter contains an **array**, it is expanded into multiple placeholders (for `WHERE IN`).
     *
     * **Examples:**
     * ```
     * // Simple positional placeholder
     * $db->prepExec('SELECT * FROM users WHERE id = ?', [5])->fetch(PDO::FETCH_ASSOC);
     *
     * // Named placeholders
     * $db->prepExec('SELECT * FROM users WHERE id = :id', ['id' => 10])->fetch(PDO::FETCH_ASSOC);
     *
     * // Multiple values for WHERE IN (named placeholders only)
     * $ids = [1, 2, 3];
     * $stmt = $db->prepExec('SELECT * FROM users WHERE id IN (:ids)', [':ids' => $ids]);
     * // SQL: SELECT * FROM users WHERE id IN (:ids0, :ids1, :ids2)
     * // Params: [":ids0" => 1, ":ids1" => 2, ":ids2" => 3]
     * ```
     *
     * @param string $sql The SQL query.
     * @param BindingsMap|string|int|null $params
     * @return PDOStatement The executed statement.
     * @throws PDOException If the query fails to execute.
     */
    protected function prepExec(string $sql, array|string|int|null $params = null): PDOStatement
    {
        [$sql, $params] = $this->convertToNamedPlaceholders($sql, (array)$params);

        if (array_filter($params, is_array(...))) {
            [$sql, $params] = $this->buildInQuery($sql, $params);
        }

        try {
            $stmt = $this->pdo()->prepare($sql);

            foreach ($params as $key => $value) {
                $paramId = is_int($key) ? $key + 1 : ':' . ltrim($key, ':');
                $stmt->bindValue($paramId, $value, match (true) {
                    is_int($value)  => PDO::PARAM_INT,
                    is_bool($value) => PDO::PARAM_BOOL,
                    $value === null      => PDO::PARAM_NULL,
                    default              => PDO::PARAM_STR
                });
            }

            $stmt->execute();
            return $stmt;
        } catch (PDOException $e) {
            if (defined('LODUR_TEST_SERVER') && LODUR_TEST_SERVER) {
                array_map(error_log(...), str_split("<br><b>{$e->getMessage()}</b><br>{$this->getPdoDebug($stmt ?? false)}", 600));
            }
            throw $e;
        }
    }


    /**
     * Get debug information from a PDO statement for error reporting.
     *
     * @param PDOStatement|false $stmt The PDO statement to debug, or false if preparation failed
     * @return string Debug information as a string, or error message if statement is false
     */
    protected function getPdoDebug(PDOStatement|false $stmt): string {
        if ($stmt === false) {
            return 'Statement preparation failed';
        }
        ob_start();
        $stmt->debugDumpParams();
        return ob_get_clean() ?: '';
    }

    /**
     * Check if a record exists in a table using a flexible WHERE condition.
     *
     * ### Usage examples (basic to advanced):
     * Classic key-value:
     * ```
     * $db->exists('users', 'id', 1);
     * ```
     * Associative array:
     * ```
     * $db->exists('users', ['status' => 'active', 'email_verified' => 1]);
     * ```
     * Associative array with raw SQL:
     * ```
     * $db->exists('users', ['status' => $status, 'email_verified != 0', 'created_at > :date'], [':date' => $date]);
     * ```
     * Custom WHERE clause with bindings:
     * ```
     * $db->exists('users', 'email = ? AND created_at > ?', ['user@example.com', '2024-01-01']);
     * ```
     * Multiple values for WHERE IN with named placeholder:
     * ```
     * $db->exists('users', 'id IN (:ids)', [':ids' => [1, 2, 3]]);
     * ```
     * Multiple values for WHERE IN with `?` placeholder:
     * ```
     * $db->exists('users', 'id IN (?)', [[1, 2, 3]]);
     * ```
     * Associative array with advanced bindings:
     * ```
     * $db->exists('users', where:[
     *  'status' => $status, // simple column & binding value
     *  'email_verified != 0', // raw sql, no binding
     *  'created_at > :date'  // raw sql with placeholder binding (ph needs to be added in bindings)
     *  'created_at > ?' => $date // new* - direct binding for ? placeholder
     * ],
     *  bindings:[':date' => $date]);
     * ```
     * @param string $table Table name
     * @param WhereInput $where Column name, condition string, or associative array
     * @param int|string|BindingsMap|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $sqlTail Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return bool True if at least one record exists, false otherwise
     * @throws PDOException
     */
    protected function exists(string $table, string|array|null $where = null, int|string|array|null $bindings = null, string|null $sqlTail = null): bool
    {
        return (bool)$this->select(
            $table,
            '1 AS `true`',
            $where,
            $bindings,
            CommonModelPicoPdoUtils::appendLimitOne($sqlTail)
        )->fetchColumn();
    }


    /**
     * Insert a new record into the table.
     * ```
     * $db->insert('users', ['name' => 'John', 'email' => 'john@example.com']);
     * // Returns the ID of the inserted record or 0 if failed
     * ```
     * Insert a new record with raw SQL:
     * ```
     * $db->insert('users', ['name' => 'John', 'created_at = NOW()', 'uuid = UUID()']);
     * ```
     * Replace a record if it already exists (Or use the wrapper for it: {@see insertReplace()}):
     * ```
     * $db->insert('users', ['name' => 'John', 'email' => 'john@example.com'], ['mode' => 'REPLACE']);
     * ```
     * Ignore the insert if the record already exists (Or use the wrapper for it: {@see insertIgnore()}):
     * ```
     * $db->insert('users', ['name' => 'John', 'email' => 'john@example.com'], ['mode' => 'INSERT IGNORE']);
     * ```
     * Insert and update if the record already exists (Or use the wrapper for it: {@see insertOnDuplicateKeyUpdate()}):
     * ```
     * $db->insert('users', ['name' => 'John', 'email' => 'john@example.com'], ['onDuplicateKeyUpdate' => ['name' => 'John', 'email' => 'john@example.com']]);
     * ```
     * Insert and get meta info:
     * ```
     * $db->insert('users', ['name' => 'John', 'email' => 'john@example.com'], ['meta' => true]);
     * // Returns ['id' => 123, 'rows' => 1, 'status' => 'inserted']
     * // Multi-row upserts use status 'affected' because PDO rowCount cannot distinguish mixed inserts/updates.
     * ```
     *  Multiple rows:
     * ```
     *    $this->insert('users', [
     *        ['name' => 'Ion', 'created_at = NOW()', 'is_active' => 1],
     *        ['name' => 'Ani', 'created_at = NOW()', 'is_active' => 0],
     *    ]);
     * ```
     *  Rows with different shapes are inserted in separate batches.
     *
     *  Rows without columns are skipped, so an empty payload is a no-op returning 0 (or status 'noop').
     *
     * @param string $table Table name
     * @param DataMap|list<DataMap> $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @param array<string, mixed>|null $options Additional options for the insert operation
     * (e.g., ['mode' => 'REPLACE'] or ['mode' => 'INSERT IGNORE'] or ['onDuplicateKeyUpdate' => ['column' => 'value']])
     * @return int|string|array<string, mixed> The ID of the inserted record, 0 if failed. For multi-statement batches, `id` is from the final INSERT statement. If 'meta' is true, returns ['id', 'rows', 'status' => 'noop|inserted|updated|affected']
     * @throws PDOException
     */
    protected function insert(string $table, array $data, array|null $options = null): int|string|array
    {
        $config = array_merge([
            'mode'                 => 'INSERT',
            'meta'                 => false, // By default returns the ID else 0. Set `true` to return ['id' => $id, 'rows' => $affectedRows, 'status' => 'noop|inserted|updated|affected']
            'onDuplicateKeyUpdate' => []
        ], (array)$options);

        $insertMode = match (strtoupper(trim((string)$config['mode']))) {
            'REPLACE'       => 'REPLACE', /*insertReplace(...)*/
            'INSERT IGNORE' => 'INSERT IGNORE', /*insertIgnore(...)*/
            default         => 'INSERT'
        };

        if ($data === []) {
            return $config['meta'] ? ['id' => 0, 'rows' => 0, 'status' => 'noop'] : 0;
        }

        $onDuplicateKeyUpdate = (array)$config['onDuplicateKeyUpdate'];
        $isMultipleInsert = CommonModelPicoPdoUtils::isRowSet($data);
        $rows = $isMultipleInsert ? array_values($data) : [$data];

        // The duplicate-update payload is repeated in every generated INSERT statement, so its
        // serialized size is fixed overhead that must be counted for every packet-sized chunk.
        $fixedBytes = $onDuplicateKeyUpdate === [] ? 0 : strlen(serialize($onDuplicateKeyUpdate));
        $chunks = CommonModelPicoPdoUtils::chunkForPacket($rows, $fixedBytes);
        $statementCount = count($chunks) > 1 || CommonModelPicoPdoUtils::hasMultipleInsertShapes($rows) ? 2 : 1;
        $rowCount = 0;

        $this->withAtomicStatements($statementCount, function () use ($chunks, $insertMode, $table, $onDuplicateKeyUpdate, &$rowCount): void {
            foreach ($chunks as $chunk) {
                foreach (CommonModelPicoPdoUtils::buildInsertBatches($chunk) as [$columns, $valueRows, $params]) {
                    $sql = "{$insertMode} INTO {$table} ({$columns}) VALUES " . implode(', ', $valueRows);
                    if ($insertMode === 'INSERT' && $onDuplicateKeyUpdate !== []) {
                        [$updateClause, $params] = $this->buildSqlClause($onDuplicateKeyUpdate, 'upd_', ', ', $params);
                        $sql .= " ON DUPLICATE KEY UPDATE {$updateClause}";
                    }
                    $rowCount += $this->prepExec($sql, $params)->rowCount();
                }
            }
        });

        $isSuccess = $rowCount > 0;
        // For a multi-statement batch this is the ID reported by the final INSERT statement,
        // not an ID range for the complete batch.
        $lastInsertId = $this->pdo()->lastInsertId() ?: 0;
        $rawId = $isSuccess ? $lastInsertId : 0;
        $id = is_numeric($rawId) ? (int)$rawId : $rawId;

        if ($config['meta'] || $isMultipleInsert) {
            $isUpsert = $insertMode === 'INSERT' && $onDuplicateKeyUpdate !== [];
            return [
                'id'     => $id,
                'rows'   => $rowCount,
                'status' => match (true) {
                    $rowCount === 0                => 'noop',
                    $isUpsert && $isMultipleInsert => 'affected',
                    $isUpsert && $rowCount > 1     => 'updated',
                    default                        => 'inserted',
                },
            ];
        }

        return $id;
    }


    /**
     * This is a wrapper around {@see insert()} that performs a `REPLACE INTO`.
     * @param string $table Table name
     * @param DataMap $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @return int|string The ID of the inserted record, 0 if failed.
     */
    protected function insertReplace(string $table, array $data): int|string
    {
        return $this->insert($table, $data, ['mode' => 'REPLACE']);
    }


    /**
     * This is a wrapper around {@see insert()} that performs an `INSERT IGNORE`.
     * ```
     * $db->insertIgnore('users', ['name' => 'John', 'email' => 'john@example.com']);
     * // If record was inserted: ['id' => 123, 'rows' => 1, 'status' => 'inserted']
     * // If record was not inserted: ['id' => 0, 'rows' => 0, 'status' => 'noop']
     * ```
     *
     * @param string $table Table name
     * @param DataMap $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @return array{id: int|string, rows: int, status: string} Inserted record meta info ['id', 'rows', 'status' => 'noop|inserted|updated']
     */
    protected function insertIgnore(string $table, array $data): array
    {
        return $this->insert($table, $data, ['mode' => 'INSERT IGNORE', 'meta' => true]);
    }


    /**
     * This is a wrapper around {@see insert()} that performs an `INSERT ... ON DUPLICATE KEY UPDATE`.
     * ```
     * $db->insertOnDuplicateKeyUpdate('users', ['name' => 'John', 'email' => 'john@example.com'], ['name' => 'John', 'email' => 'john@example.com']);
     * // If record was inserted: ['id' => 123, 'rows' => 1, 'status' => 'inserted']
     * // If record was updated: ['id' => 0, 'rows' => 2, 'status' => 'updated']
     * // If record was same: ['id' => 0, 'rows' => 0, 'status' => 'noop']
     * ```
     *
     * @param string $table Table name
     * @param DataMap $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @param DataMap|null $onDuplicateKeyUpdate Key-value pairs of column names and values to update on duplicate key or raw sql queries like 'date = NOW()'. If null/empty, the $data array will be used
     * @return array{id: int|string, rows: int, status: string} Inserted record meta info ['id', 'rows', 'status' => 'noop|inserted|updated']
     */
    protected function insertOnDuplicateKeyUpdate(string $table, array $data, array|null $onDuplicateKeyUpdate = null): array
    {
        return $this->insert($table, $data, ['onDuplicateKeyUpdate' => $onDuplicateKeyUpdate ?: $data, 'meta' => true]);
    }


    /**
     * Update table records by flexible WHERE conditions.
     *
     * Single updates build classic `SET col = :set_col WHERE …` SQL. Batch updates (parallel `$data` / `$where`
     * lists) compile to one `CASE WHEN` query per column. `$sqlTail` is appended to the whole statement
     * (e.g. `ORDER BY id LIMIT 10`). A packet-split batch rejects `LIMIT`, because applying the
     * same limit independently to every chunk would exceed the caller's requested total.
     *
     * ### Usage examples:
     * Classic key-value:
     * ```
     * $db->update('users', ['name' => 'John'], 'id', 1);
     * ```
     * Key-value with raw SQL:
     * ```
     * $db->update('users', ['name' => 'John', 'date_verified = NOW()'], 'id', 1);
     * ```
     * Associative WHERE:
     * ```
     * $db->update('users', ['name' => 'John'], ['id' => 1, 'active' => 1]);
     * ```
     * Associative WHERE with raw SQL:
     * ```
     * $db->update('users', ['name' => 'John'], ['id' => 1, 'email_verified != 0', 'created_at > :date'], [':date' => $date]);
     * ```
     * Custom WHERE clause:
     * ```
     * $db->update('users', ['name' => 'John'], 'email = ? OR status = ?', ['john@example.com', 'active']);
     * ```
     * Multiple values for WHERE IN with named placeholders
     * ```
     * $db->update('users', ['name' => 'John'], 'id IN (:ids)', [':ids' => [1, 2, 3]]);
     * ```
     * Multiple values for WHERE IN with `?` placeholder:
     * ```
     * $db->update('users', ['name' => 'John'], 'id IN (?)', [[1, 2, 3]]);
     * ```
     * With sqlTail:
     * ```
     * $db->update('users', ['status' => 'archived'], ['status' => 'inactive'], null, 'ORDER BY id LIMIT 10');
     * ```
     * Multi update (parallel `$data` / `$where` / `$bindings` lists — two rows below cover the main SET & WHERE patterns):
     * ```
     * $since = '2024-01-01';
     * $teamIds = [10, 11, 12];
     * $data = [
     *     ['name' => 'Alice', 'views = views + 1'],           // raw SQL in SET (numeric key, same as insert)
     *     ['name' => 'Bob', 'views = views + ?' => 3],        // `?` key in SET binds the increment
     * ];
     * $where = [
     *     ['id' => 1, 'email_verified != 0', 'created_at > :since', 'id IN (:ids)'],
     *     'id = ? AND role = ?',
     * ];
     * $bindings = [
     *     [':since' => $since, ':ids' => $teamIds],
     *     [99, 'editor'],
     * ];
     * $db->update('users', $data, $where, $bindings);
     * ```
     * Associative array with advanced bindings:
     * ```
     * $db->update('users', ['name' => 'John'], [
     *  'status' => $status,
     *  'email_verified != 0',
     *  'created_at > :date',
     *  'created_at > ?' => $date,
     * ], [':date' => $date]);
     * ```
     *
     * @param string $table Table name
     * @param DataMap|list<DataMap> $data SET map, or list of SET maps when `$where` is also a list (batch)
     * @param WhereInput|list<DataMap|string> $where Column/condition (single), or list aligned with `$data` (batch)
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings Per-row list when batch `$where` uses `?` / named placeholders
     * @param string|null $sqlTail Extra query suffix (e.g. ORDER BY, LIMIT)
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function update(string $table, array $data, string|array|null $where = null, int|string|array|null $bindings = null, string|null $sqlTail = null): int
    {
        if ($data === []) {
            throw new InvalidArgumentException('UPDATE data cannot be empty');
        }

        if (CommonModelPicoPdoUtils::isRowSet($data)) {
            $data = array_values($data);

            if (!is_array($where)
                || !CommonModelPicoPdoUtils::hasOnlyIntKeys($where)
                || count($data) !== count($where)) {
                throw new InvalidArgumentException(
                    'Batch UPDATE requires one integer-keyed WHERE condition per data row'
                );
            }

            $where = array_values($where);
            foreach ($data as $row) {
                if ($row === []) {
                    throw new InvalidArgumentException('Batch UPDATE rows cannot be empty');
                }
            }
            foreach ($where as $condition) {
                if ($condition === [] || (is_string($condition) && trim($condition) === '')) {
                    throw new InvalidArgumentException('Batch UPDATE conditions cannot be empty');
                }
            }

            $bindings = CommonModelPicoPdoUtils::normalizePositionalBindings($bindings);
            $perRowBindings = is_array($bindings) && $bindings !== [] && array_is_list($bindings);
            $packetRows = [];
            foreach ($data as $i => $row) {
                $packetRows[] = [
                    $row,
                    $where[$i],
                    $perRowBindings ? ($bindings[$i] ?? null) : null,
                ];
            }
            $fixedBytes = $perRowBindings ? 0 : strlen(serialize($bindings));
            $chunks = CommonModelPicoPdoUtils::chunkForPacket($packetRows, $fixedBytes);

            if (count($chunks) > 1 && CommonModelPicoPdoUtils::hasLimit($sqlTail)) {
                throw new InvalidArgumentException(
                    'LIMIT cannot be combined with a packet-split batch UPDATE'
                );
            }

            if (count($chunks) > 1) {
                $rowCount = 0;
                $this->withAtomicStatements(count($chunks), function () use ($chunks, &$rowCount, $table, $data, $where, $bindings, $sqlTail): void {
                    $offset = 0;
                    foreach ($chunks as $chunk) {
                        $length = count($chunk);
                        $rowCount += $this->update(
                            $table,
                            CommonModelPicoPdoUtils::chunkList($data, $offset, $length),
                            CommonModelPicoPdoUtils::chunkList($where, $offset, $length),
                            CommonModelPicoPdoUtils::chunkList($bindings, $offset, $length),
                            $sqlTail
                        );
                        $offset += $length;
                    }
                });

                return $rowCount;
            }

            $tempPlan = CommonModelPicoPdoUtils::buildTempTableUpdate($table, $data, $where, $bindings, $sqlTail);
            if ($tempPlan !== []) {
                return $this->tempTableUpdate($data, $tempPlan);
            }

            [$setClause, $whereClause, $params] = $this->buildUpdateSqlParts($data, $where, $bindings);
        } else {
            // Single row: classic UPDATE with the familiar :set_* / :where_* placeholders.
            [$setClause, $params] = $this->buildSqlClause($data, 'set_', ', ');
            [$whereClause, $whereParams] = $this->buildWhereQuery($where, $bindings);
            $params = array_merge($params, $whereParams);
        }

        $whereClause = CommonModelPicoPdoUtils::requireWhereClause($whereClause, 'UPDATE');
        $sql = implode(' ', array_filter(array_map(trim(...), ['UPDATE', $table, 'SET', $setClause, $whereClause, (string)$sqlTail])));
        return $this->prepExec($sql, $params)->rowCount();
    }


    /**
     * Select rows from a table using flexible WHERE conditions.
     * It returns a PDOStatement object that can be used to fetch results.
     *
     * ### Usage examples:
     * Classic key-value:
     * ```
     * $db->select('users', 'id, name', 'id', 1)->fetch(PDO::FETCH_ASSOC);
     * ```
     * Classic key-value with limit etc:
     * ```
     * $db->select('users', 'id, name', 'status', 'active', 'ORDER BY name LIMIT 1')->fetch(PDO::FETCH_ASSOC);
     * ```
     * Associative WHERE:
     * ```
     * $db->select('users', 'id, name', ['status' => 'active', 'email_verified' => 1])->fetch(PDO::FETCH_ASSOC);
     * ```
     * Associative WHERE with raw SQL:
     * ```
     * $db->select('users', 'id, name', ['status' => 'active', 'email_verified != 0', 'created_at > :date'], [':date' => $date])->fetch(PDO::FETCH_ASSOC);
     * ```
     * Custom WHERE clause with bindings:
     * ```
     * $db->select('users', 'id, name', 'last_login < ? AND status != ?', ['2023-01-01', 'active'])->fetch(PDO::FETCH_ASSOC);
     * ```
     * Multiple values for WHERE IN with named placeholders
     * ```
     * $db->select('users', 'id, name', 'id IN (:ids)', [':ids' => [1, 2, 3]])->fetch(PDO::FETCH_ASSOC);
     * ```
     * Multiple values for WHERE IN with `?` placeholder:
     * ```
     * $db->select('users', 'id, name', 'id IN (?)', [[1, 2, 3]])->fetchAll(PDO::FETCH_ASSOC);
     * ```
     * Associative array with advanced bindings:
     * ```
     * $db->select('users', 'id, name', [
     *  'status' => $status,
     *  'email_verified != 0',
     *  'created_at > :date',
     *  'created_at > ?' => $date,
     * ], [':date' => $date])->fetchAll(PDO::FETCH_ASSOC);
     * ```
     * @param string $table Table name
     * @param list<string>|string|int|null $columns Columns to select (default '*')
     * @param WhereInput $where Column name, condition string, or associative array
     * @param int|string|BindingsMap|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $sqlTail Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return PDOStatement
     */
    protected function select(string $table, array|string|int|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $sqlTail = null): PDOStatement
    {
        $columnList = implode(', ', is_array($columns) ? ($columns ?: ['*']) : [$columns ?: '*']);
        [$whereClause, $params] = $this->buildWhereQuery($where, $bindings);
        $whereClause = trim($whereClause);
        if ($whereClause !== '' && preg_match('/^WHERE\b/i', $whereClause) !== 1) {
            $whereClause = 'WHERE ' . $whereClause;
        }
        $sql = implode(' ', array_filter(array_map(trim(...), ['SELECT', $columnList, 'FROM', $table, $whereClause, (string)$sqlTail])));
        return $this->prepExec($sql, $params);
    }


    /**
     * Wrapper for {@see select()} to fetch one row only, LIMIT 1 is appended automatically.
     *
     * @param string $table Table name
     * @param list<string>|string|int|null $columns Columns to select (default '*')
     * @param WhereInput $where Column name, condition string, or associative array
     * @param int|string|BindingsMap|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $sqlTail Extra query suffix (e.g. GROUP BY, ORDER BY .. ) LIMIT 1 is appended automatically.
     * @return array<string, mixed>
     */
    protected function selectOne(string $table, array|string|int|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $sqlTail = null): array
    {
        return $this->select(
            $table,
            $columns,
            $where,
            $bindings,
            CommonModelPicoPdoUtils::appendLimitOne($sqlTail)
        )->fetch(PDO::FETCH_ASSOC) ?: [];
    }

    /**
     * Wrapper for {@see select()} to fetch all rows.
     *
     * @param string $table Table name
     * @param list<string>|string|int|null $columns Columns to select (default '*')
     * @param WhereInput $where Column name, condition string, or associative array
     * @param int|string|BindingsMap|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $sqlTail Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return list<array<string, mixed>>
     */
    protected function selectAll(string $table, array|string|int|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $sqlTail = null): array
    {
        return $this->select($table, $columns, $where, $bindings, $sqlTail)->fetchAll(PDO::FETCH_ASSOC);
    }


    /**
     * Select rows by merging reusable SQL fragments, then executing via {@see select()}.
     *
     * Use when a listing query is built from optional pieces (core columns, filters, extras)
     * that should be appended independently without concatenating JOIN/WHERE strings by hand.
     *
     * ### Fragment shape
     * Each fragment is an associative array with optional keys:
     * - `select` — list of SELECT expressions (columns, aliases, subqueries)
     * - `joins` — list of full JOIN clauses (`LEFT JOIN … ON …`), order preserved
     * - `where` — {@see DataMap} passed to {@see buildSqlClause()} (equality, `?` keys, raw numeric entries)
     * - `bindings` — {@see BindingsMap} for named placeholders in that fragment's `where` (or SELECT)
     *
     * ```
     * [
     *     'select'   => ['event.event_id', 'event.date_start'],
     *     'joins'    => ['LEFT JOIN event_to_tab ON event_to_tab.event_id = event.event_id'],
     *     'where'    => [
     *         'event.fw_id' => $fwId,
     *         'event.date_start >= ?' => $dateFrom,
     *         'event.status != 0',
     *     ],
     *     'bindings' => [':named' => $value],
     * ]
     * ```
     *
     * ### Behaviour
     * - `select` / `joins` from all fragments are concatenated; exact duplicate lines are removed
     * - each fragment's `where` becomes one parenthesized AND-group; groups are AND-joined
     * - per-fragment parameter prefixes (`frag_{n}_`) avoid placeholder clashes from {@see buildSqlClause()}
     * - bindings from all fragments are merged (later fragment wins on the same name)
     * - `$table` is the FROM expression **without** the `FROM` keyword (joins from fragments are appended)
     * - `$sqlTail` is appended as-is (e.g. full `ORDER BY …`, `GROUP BY …`, `LIMIT …`)
     * - empty `select` across all fragments falls back to `*` via {@see select()}
     *
     * ### Usage examples
     *
     * Core columns + year/org filter + order:
     * ```
     * $db->selectCompose(
     *     'uebungs_programm LEFT JOIN event ON event.event_id = uebungs_programm.event_id',
     *     [
     *         ['select' => ['event.event_id', 'event.date_start'], 'joins' => ['LEFT JOIN arbeitsrapporte ON …']],
     *         ['where' => ['event.fw_id' => $fwId, 'event.date_start >= ?' => $yearStart, 'event.date_start < ?' => $yearEnd]],
     *     ],
     *     'ORDER BY event.date_start, event.event_id'
     * )->fetchAll(PDO::FETCH_ASSOC);
     * ```
     *
     * Optional filter fragment (omit the array entry when not needed):
     * ```
     * $fragments = [$this->fragCoreListing(), $this->fragWhereYearOrg($year, $fwId)];
     * if ($filterToPerson) {
     *     $fragments[] = $this->fragPersonEventFilter($mannschaftId);
     * }
     * $db->selectCompose($from, $fragments, $this->sortByListing());
     * ```
     *
     * Raw WHERE + named bindings (complex OR / subquery):
     * ```
     * $db->selectCompose($from, [
     *     $this->fragCoreListing(),
     *     [
     *         'where' => ['(event.event_id IN (SELECT … WHERE id = :mannschaft_id) OR …)'],
     *         'bindings' => [':mannschaft_id' => $mannschaftId],
     *     ],
     * ], 'ORDER BY event.date_start');
     * ```
     *
     * Bindings for placeholders that live in SELECT (no WHERE on that fragment):
     * ```
     * $db->selectCompose($from, [
     *     $this->fragCoreListing(),
     *     [
     *         'select' => ['(SELECT … WHERE ATD.what_id = :subwhat) AS senden'],
     *         'bindings' => [':subwhat' => $subwhat],
     *     ],
     * ]);
     * ```
     *
     * Alternate FROM (event-first) with IN list:
     * ```
     * $db->selectCompose(
     *     'event LEFT JOIN uebungs_programm ON uebungs_programm.event_id = event.event_id',
     *     [
     *         $this->fragCoreListing(),
     *         ['where' => ['event.event_art_id IN (:ids)'], 'bindings' => [':ids' => $eventArtIds]],
     *     ],
     *     'ORDER BY event.date_start'
     * );
     * ```
     *
     * @param string $table FROM expression without the FROM keyword (may include base JOINs)
     * @param list<array{
     *     select?: list<string>,
     *     joins?: list<string>,
     *     where?: DataMap,
     *     bindings?: BindingsMap
     * }> $fragments Ordered fragment pieces to merge
     * @param string|null $sqlTail Trailing SQL appended after WHERE (full `ORDER BY` / `GROUP BY` / `LIMIT`)
     * @return PDOStatement Executed statement — fetch with `fetch` / `fetchAll` / `FETCH_CLASS` as needed
     * @throws PDOException
     */
    protected function selectCompose(string $table, array $fragments, string|null $sqlTail = null): PDOStatement
    {
        $select = [];
        $joins = [];
        $where = [];
        $params = [];

        foreach ($fragments as $index => $fragment) {
            array_push($select, ...(array)($fragment['select'] ?? []));
            array_push($joins, ...(array)($fragment['joins'] ?? []));

            [$whereSql, $fragmentParams] = $this->buildSqlClause(
                (array)($fragment['where'] ?? []),
                "frag_{$index}_",
                ' AND ',
                (array)($fragment['bindings'] ?? [])
            );

            if ($whereSql !== '') {
                $where[] = "({$whereSql})";
            }
            $params = array_merge($params, $fragmentParams);
        }

        $select = array_values(array_unique($select));
        $joins = array_values(array_unique($joins));

        return $this->select(
            implode(PHP_EOL, [$table, ...$joins]),
            $select ?: null,
            $where,
            $params,
            $sqlTail
        );
    }

    /**
     * Delete rows from a table using flexible WHERE conditions.
     *
     * ### Usage examples:
     * Classic key-value:
     * ```
     * $db->delete('users', 'id', 1);
     * ```
     * Associative WHERE:
     * ```
     * $db->delete('users', ['status' => 'inactive', 'email_verified' => 0]);
     * ```
     * Associative WHERE with raw SQL:
     * ```
     * $db->delete('users', ['status' => 'inactive', 'email_verified != 0', 'created_at > :date'], [':date' => $date]);
     * ```
     * Custom WHERE clause with bindings:
     * ```
     * $db->delete('users', 'last_login < ? AND status != ?', ['2023-01-01', 'active']);
     * ```
     * Multiple values for WHERE IN (named placeholders only)
     * ```
     * $db->delete('users', 'id IN (:ids)', [':ids' => [1, 2, 3]]);
     * ```
     * Batch delete (list of WHERE maps / conditions — one OR-group per entry):
     * ```
     * $db->delete('kurs_teilnehmer', [
     *     ['kurspool_id' => 1, 'kurs_id' => 2, 'fw_id' => 3, 'mannschaft_id' => 4],
     *     ['kurspool_id' => 1, 'kurs_id' => 2, 'fw_id' => 5, 'mannschaft_id' => 6],
     * ]);
     * // DELETE FROM kurs_teilnehmer WHERE (kurspool_id = … AND …) OR (kurspool_id = … AND …)
     * ```
     * Batch with per-row bindings + sqlTail:
     * ```
     * $db->delete('users', [
     *     ['id = ?' => 1],
     *     ['id = ?' => 2],
     * ], null, 'LIMIT 10');
     * ```
     *
     * @param string $table Table name
     * @param string|DataMap|list<DataMap|string> $where Column/condition (single), assoc map, or list of maps/conditions (batch)
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings Value / map for single; per-row list when batch
     * @param string|null $sqlTail Extra query suffix (e.g. ORDER BY, LIMIT)
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function delete(string $table, string|array $where, int|string|array|null $bindings = null, string|null $sqlTail = null): int
    {
        if (CommonModelPicoPdoUtils::isRowSet($where)) {
            $where = array_values($where);
            foreach ($where as $condition) {
                if ($condition === []) {
                    throw new InvalidArgumentException('Batch DELETE conditions cannot be empty');
                }
            }

            $bindings = CommonModelPicoPdoUtils::normalizePositionalBindings($bindings);
            $perRowBindings = is_array($bindings) && $bindings !== [] && array_is_list($bindings);
            $packetRows = [];
            foreach ($where as $i => $condition) {
                $packetRows[] = [
                    $condition,
                    $perRowBindings ? ($bindings[$i] ?? null) : null,
                ];
            }
            $fixedBytes = $perRowBindings ? 0 : strlen(serialize($bindings));
            $whereChunks = CommonModelPicoPdoUtils::chunkForPacket($packetRows, $fixedBytes);

            if (count($whereChunks) > 1 && CommonModelPicoPdoUtils::hasLimit($sqlTail)) {
                throw new InvalidArgumentException(
                    'LIMIT cannot be combined with a packet-split batch DELETE'
                );
            }

            if (count($whereChunks) > 1) {
                $rowCount = 0;
                $this->withAtomicStatements(count($whereChunks), function () use ($whereChunks, &$rowCount, $table, $where, $bindings, $sqlTail): void {
                    $offset = 0;
                    foreach ($whereChunks as $chunk) {
                        $length = count($chunk);
                        $rowCount += $this->delete(
                            $table,
                            CommonModelPicoPdoUtils::chunkList($where, $offset, $length),
                            CommonModelPicoPdoUtils::chunkList($bindings, $offset, $length),
                            $sqlTail
                        );
                        $offset += $length;
                    }
                });

                return $rowCount;
            }

            [$whereClause, $params] = $this->buildDeleteSqlParts($where, $bindings);
        } else {
            [$whereClause, $params] = $this->buildWhereQuery($where, $bindings);
        }

        $whereClause = CommonModelPicoPdoUtils::requireWhereClause($whereClause, 'DELETE');
        $sql = implode(' ', array_filter(array_map(trim(...), ['DELETE FROM', $table, $whereClause, (string)$sqlTail])));
        return $this->prepExec($sql, $params)->rowCount();
    }


    /**
     * Expands named placeholders in SQL queries for `IN` clauses.
     *
     * - Replaces named placeholders (`:key`) with multiple placeholders (`:key0, :key1, :key2`) for `WHERE IN` queries.
     * - Updates the bind parameters to match the expanded placeholders.
     * - Supports named placeholders for arrays. Positional `IN (?)` is first converted by {@see convertToNamedPlaceholders()}.
     * - Empty arrays throw rather than generating invalid or surprising `IN ()` / `IN (NULL)` SQL.
     * **Examples:**
     * ```
     * $sql = 'SELECT * FROM users WHERE id IN (:ids)';
     * $params = ['ids' => [1, 2, 3]];
     *
     * [$newSql, $newParams] = $this->buildInQuery($sql, $params);
     *
     * // Resulting SQL: "SELECT * FROM users WHERE id IN (:ids0, :ids1, :ids2)"
     * // Resulting Params: [":ids0" => 1, ":ids1" => 2, ":ids2" => 3]
     * ```
     *
     * @param string $sql The SQL query containing named placeholders.
     * @param BindingsMap $params The parameters to bind, where array values are expanded.
     * @return array{0: string, 1: BindingsMap} The modified SQL query and the updated bind parameters.
     */
    protected function buildInQuery(string $sql, array $params): array
    {
        if ($sql === '' || $params === []) {
            return [$sql, $params];
        }

        $expandedParams = [];

        foreach ($params as $key => $value) {
            if (!is_array($value)) {
                $expandedParams[$key] = $value;
                continue;
            }

            if (!is_string($key)) {
                throw new InvalidArgumentException(
                    'Array bindings require a named placeholder'
                );
            }

            if ($value === []) {
                throw new InvalidArgumentException(
                    "Array binding {$key} cannot be empty"
                );
            }

            $searchKey = ':' . ltrim($key, ':');
            $valuesIn = array_values($value);
            $placeholders = array_map(
                static fn(int $index): string => "{$searchKey}{$index}",
                array_keys($valuesIn)
            );

            // Match :key followed by a word boundary to prevent partial matches.
            // This ensures :ids doesn't match :ids_extra or :ids2.
            $pattern = '/(?<!:)' . preg_quote($searchKey, '/') . '\\b/';
            $sql = preg_replace($pattern, implode(',', $placeholders), $sql, -1, $replacementCount);

            if ($replacementCount === 0) {
                throw new InvalidArgumentException(
                    "Array binding {$searchKey} has no matching SQL placeholder"
                );
            }

            foreach ($placeholders as $index => $placeholder) {
                $expandedParams[$placeholder] = $valuesIn[$index];
            }
        }

        return [$sql, $expandedParams];
    }


    /**
     * Build SQL clauses and parameters from mixed data array, preserving input order and converting bindings to named placeholders.
     * ```
     * buildSqlClause(['name' => 'John', 'email' => 'john@example.com', 'created_at = NOW()'], 'insert_');
     * //sql: name = :insert_name, email = :insert_email, created_at = NOW()
     * //params: [':insert_name' => 'John', ':insert_email' => 'john@example.com']
     * ```
     * With AND joiner
     * ```
     * buildSqlClause(['name' => 'John', 'email' => 'john@example.com', 'created_at = NOW()'], 'insert_', ' AND ');
     * //sql: name = :insert_name AND email = :insert_email AND created_at = NOW()
     * //params: [':insert_name' => 'John', ':insert_email' => 'john@example.com']
     * ```
     * With ? placeholders converted to named:
     * ```
     * buildSqlClause(['name' => 'John', 'email' => 'john@example,com', 'last_login = ?' => '2024-01-01'], 'update_', ', ');
     * //sql: name = :update_name, email = :update_email, last_login = :update_0
     * //params: [':update_name' => 'John', ':update_email' => 'john@example,com', ':update_0' => '2024-01-01']
     * ```
     *
     * Dots in column keys (e.g. `u.id`) are kept in the SQL fragment; the bound name uses `_dot_` (e.g. `:where_u_dot_id`).
     *
     * @param DataMap $data Mixed array of key-value pairs and raw SQL strings
     * @param string|null $prefix Parameter prefix (e.g., 'set_', 'insert_', 'update_')
     * @param string|null $joiner How to join the clauses (e.g., ', ', ' AND ', ' OR ')
     * @param BindingsMap $bindings Additional bindings for raw SQL entries not listed in $data, pass through in mind.
     * @return array{0: string, 1: BindingsMap} [sqlClause, parameters]
     */
    protected function buildSqlClause(array $data, string|null $prefix = null, string|null $joiner = null, array $bindings = []): array
    {
        $joiner ??= ', ';
        $clauses = [];
        $params = [];
        $questionBindings = [];
        $rawPositionalBindings = [];
        $namedBindings = [];

        foreach ($bindings as $key => $value) {
            if (is_int($key)) {
                $rawPositionalBindings[] = $value;
            } else {
                $namedBindings[$key] = $value;
            }
        }

        $rawBindingIndex = 0;

        foreach ($data as $key => $value) {
            if (is_numeric($key)) {
                // Raw SQL like "updated_at = NOW()" or "date > ?". Positional values for raw
                // fragments are consumed from $bindings in the same order as the fragments.
                $clause = (string)$value;
                $questionCount = substr_count($clause, '?');
                for ($i = 0; $i < $questionCount; $i++) {
                    if (!array_key_exists($rawBindingIndex, $rawPositionalBindings)) {
                        throw new InvalidArgumentException(
                            'Not enough positional bindings for raw SQL clause placeholders'
                        );
                    }
                    $questionBindings[] = $rawPositionalBindings[$rawBindingIndex++];
                }
                $clauses[] = $clause;
                continue;
            }

            if (str_contains($key, '?')) {
                // A keyed `?` entry binds its value directly. Use a numeric raw SQL entry plus
                // $bindings when one clause contains more than one positional placeholder.
                if (substr_count($key, '?') !== 1) {
                    throw new InvalidArgumentException(
                        'A keyed SQL clause must contain exactly one ? placeholder'
                    );
                }
                $clauses[] = $key;
                $questionBindings[] = $value;
                continue;
            }

            // Key-value pair like "users.name" => "John".
            $keySafe = str_replace('.', '_dot_', $key);
            $placeholder = ":{$prefix}{$keySafe}";
            $clauses[] = "{$key} = {$placeholder}";
            $params[$placeholder] = $value;
        }

        if ($rawBindingIndex < count($rawPositionalBindings)) {
            throw new InvalidArgumentException(
                'More positional clause bindings were provided than SQL placeholders'
            );
        }

        [$sql, $questionParams] = $this->convertToNamedPlaceholders(
            implode($joiner, $clauses),
            $questionBindings,
            $prefix
        );

        return [
            $sql,
            array_merge($params, $questionParams, $namedBindings),
        ];
    }

    /**
     * Builds a SQL `WHERE` clause and its corresponding named parameter bindings
     * from flexible inputs.
     *
     * Supports:
     * - Associative arrays for simple equality comparisons
     * - Raw condition strings with:
     *   - `?` placeholders (auto-converted to named `:where_0`, `:where_1`, etc.)
     *   - Named placeholders (e.g. `:role`, `:status`, `:ids`) used as-is or expanded
     * - Single column/value shorthand
     * - Raw SQL condition without bindings
     * - Automatic expansion of arrays for `IN (:placeholder)` and `IN (?)` patterns
     *
     * UPDATE/DELETE validate the compiled result and throw `InvalidArgumentException` when
     * no WHERE clause is provided, preventing accidental full-table operations.
     *
     * ### Examples (increasing complexity):
     *
     * 1. Single key-value pair
     * ```
     * buildWhereQuery('id', 1);
     * // id = :where_id
     * // [':where_id' => 1]
     *
     * buildWhereQuery('id', 1, 'b0_w_'); // custom prefix (batch rows, etc.)
     * // id = :b0_w_id
     * ```
     *
     * 2.a Multiple key-value conditions
     * ```
     * buildWhereQuery(['id' => 1, 'status' => 'active']);
     * // id = :where_id AND status = :where_status
     * // [':where_id' => 1, ':where_status' => 'active']
     * ```
     *
     * 2.b Multiple key-value conditions with raw SQL as numeric key entries
     * ```
     * buildWhereQuery(['id' => 1, 'status' => 'active', 'email_verified != 0', 'created_at > :date'], [':date' => $date]);
     * // id = :where_id AND status = :where_status AND email_verified != 0 AND created_at > :date
     * // [':where_id' => 1, ':where_status' => 'active', ':date' => $date]
     * ```
     *
     * 3. Raw SQL with `?` placeholders and bindings
     * ```
     * buildWhereQuery('email = ? AND status != ?', ['john@example.com', 'inactive']);
     * // email = :where_0 AND status != :where_1
     * // [':where_0' => 'john@example.com', ':where_1' => 'inactive']
     * ```
     *
     * 4. Raw SQL with named placeholders
     * ```
     * buildWhereQuery('role = :role AND status = :status', [':role' => 'admin', ':status' => 'active']);
     * // role = :role AND status = :status
     * // [':role' => 'admin', ':status' => 'active']
     * ```
     *
     * 5. Raw SQL with `IN (:placeholder)` and array binding
     * ```
     * buildWhereQuery('id IN (:ids)', [':ids' => [1, 2, 3]]);
     * // id IN (:ids0, :ids1, :ids2)
     * // [':ids0' => 1, ':ids1' => 2, ':ids2' => 3]
     * ```
     *
     * 6. Raw SQL with no bindings
     * ```
     * buildWhereQuery('created_at IS NULL');
     * // created_at IS NULL
     * // []
     * ```
     * 7. Raw SQL with direct bindings
     * ```
     * buildWhereQuery(['id IN (?)' => $ids, 'created_at > ?' => $date]);
     * ```
     *
     * @param WhereInput $where Column name, condition string, or associative array
     * @param int|string|BindingsMap|null $bindings Value for single column or array of bound values for custom condition
     * @param string $prefix Named-placeholder prefix for generated binds (default `where_`; batch rows use e.g. `b0_w_`)
     * @return array{0: string, 1: BindingsMap} The WHERE clause and parameter bindings
     */
    protected function buildWhereQuery(string|array|null $where = null, int|string|array|null $bindings = null, string $prefix = 'where_'): array
    {
        if (empty($where)) {
            return ['', (array)$bindings];
        }

        if (is_array($where)) {
            [$where, $bindings] = $this->buildSqlClause($where, $prefix, ' AND ', (array)$bindings);
        }

        if (str_contains($where, '?')) {
            [$where, $bindings] = $this->convertToNamedPlaceholders($where, (array)$bindings, $prefix);
        }

        // Array bindings are intentionally expanded only once, in prepExec(), after SELECT,
        // JOIN, WHERE and SQL-tail fragments have been assembled into the final SQL string.
        if (str_contains($where, ':')) {
            return [$where, (array)$bindings];
        }

        if (is_scalar($bindings)) {
            $keySafe = str_replace('.', '_dot_', (string)$where);
            $param = ":{$prefix}{$keySafe}";

            return ["{$where} = {$param}", [$param => $bindings]];
        }

        return [$where, (array)$bindings];
    }

    /**
     * Convert positional `?` placeholders in a query to named placeholders.
     * Replaces each `?` with a named placeholder like `:nph_0`, `:nph_1`, etc.
     * and updates the bindings array accordingly.
     * ```
     * $query = 'SELECT * FROM users WHERE id = ? AND status = ?';
     * $bindings = [5, 'active'];
     * [$newQuery, $newBindings] = $this->convertToNamedPlaceholders($query, $bindings);
     * // $newQuery: 'SELECT * FROM users WHERE id = :nph_0 AND status = :nph_1'
     * // $newBindings: [':nph_0' => 5, ':nph_1' => 'active']
     * ```
     * @param string $query
     * @param BindingsMap $bindings
     * @param string|null $prefix
     * @return array{0: string, 1: BindingsMap} [modified query, updated bindings]
     */
    protected function convertToNamedPlaceholders(string $query, array $bindings, string|null $prefix = null): array
    {
        $prefix = $prefix ?: 'nph_';
        $i = 0;
        $clause = preg_replace_callback('/\?/', static function () use ($prefix, &$i, &$bindings) {
            $param = ":{$prefix}{$i}";
            if (array_key_exists($i, $bindings)) { // Let PDO handle missing bindings
                $bindings[$param] = $bindings[$i];
                unset($bindings[$i]); // Remove the used binding
            }
            $i++;
            return $param;
        }, $query);

        return [$clause, $bindings];
    }


    /**
     * Applies a batch update by staging the rows in a temporary table and joining against it.
     *
     * The values move out of the SQL text and into data, so the UPDATE is one fixed-size
     * statement however many rows are involved, and the server resolves each row with an index
     * lookup instead of walking a CASE list.
     *
     * Shared where predicates from {@see CommonModelPicoPdoUtils::buildTempTableUpdate()} are
     * already qualified onto alias `t`; their user-supplied `:name` binds are always isolated
     * under the `tmp_c_` prefix so they cannot collide with other clauses. Per-row plain
     * equalities are staged.
     *
     * The staging table is created with `AS SELECT ... WHERE 1 = 0`, which copies the target's
     * exact column types and nullability without its constraints or defaults; the join index is
     * declared inline so no ALTER is needed. Temporary tables are private to this connection and
     * do not trigger an implicit commit, so the whole sequence stays inside one transaction.
     *
     * @param list<DataMap> $data
     * @param array{
     *     target: string,
     *     rowWhere: list<DataMap>,
     *     common: array<string|int, mixed>,
     *     bindings: BindingsMap
     * } $plan
     * @return int Rows actually changed, matching the CASE/WHEN path.
     */
    private function tempTableUpdate(array $data, array $plan): int
    {
        $target = $plan['target'];
        $keys = array_map(CommonModelPicoPdoUtils::quoteIdentifier(...), array_keys($plan['rowWhere'][0]));
        $cols = array_map(CommonModelPicoPdoUtils::quoteIdentifier(...), array_keys($data[0]));
        $staging = '`tmp_pico_update_' . uniqid() . '`';

        $columnList = implode(', ', [...$keys, ...$cols]);
        $pair = static fn(string $column): string => "t.{$column} = x.{$column}";

        $rows = [];
        foreach ($data as $i => $row) {
            $rows[] = $plan['rowWhere'][$i] + $row;
        }

        $updateWhere = array_map($pair, $keys);
        $updateBindings = null;

        if ($plan['common'] !== []) {
            [$commonSql, $commonParams] = $this->buildWhereQuery($plan['common'], $plan['bindings'], 'tmp_c_');
            [$commonSql, $updateBindings] = CommonModelPicoPdoUtils::isolateNamedPlaceholders(
                $commonSql,
                $commonParams,
                'tmp_c_'
            );
            $updateWhere[] = $commonSql;
        }

        $rowCount = 0;

        $this->withAtomicStatements(2, function () use (
            $staging,
            $keys,
            $columnList,
            $target,
            $rows,
            &$rowCount,
            $pair,
            $cols,
            $updateWhere,
            $updateBindings
        ): void {
            $this->pdo()->exec("DROP TEMPORARY TABLE IF EXISTS {$staging}");
            $this->pdo()->exec(
                "CREATE TEMPORARY TABLE {$staging} (INDEX idx_join (" . implode(', ', $keys) . '))'
                . " AS SELECT {$columnList} FROM {$target} WHERE 1 = 0"
            );

            try {
                $this->insert($staging, $rows);
                $rowCount = $this->update(
                    "{$target} t JOIN {$staging} x",
                    array_map($pair, $cols),
                    $updateWhere,
                    $updateBindings
                );
            } finally {
                $this->pdo()->exec("DROP TEMPORARY TABLE IF EXISTS {$staging}");
            }
        });

        return $rowCount;
    }


    /**
     * Compiles parallel delete WHERE rows into [WHERE clause, params] for batch {@see delete()}.
     *
     * @param list<DataMap|string> $whereRows
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings
     * @return array{0: string, 1: BindingsMap}
     */
    private function buildDeleteSqlParts(array $whereRows, int|string|array|null $bindings): array
    {
        $params = [];
        $wheres = [];
        foreach ($whereRows as $i => $row) {
            $wheres[$i] = $this->compileBatchRowWhere($row, $bindings, $i, $params);
        }

        return [
            implode(' OR ', array_map(static fn(string $w): string => "({$w})", $wheres)),
            $params,
        ];
    }

    /**
     * Compiles one batch row's WHERE and merges its binds into the statement params.
     *
     * A bindings list is sliced by row index; anything else (shared map / scalar) applies to
     * every row. User-supplied `:name` binds that an earlier row already bound to a different
     * value are renamed under `b{$i}_n_` so each row keeps its own value; a name re-bound to
     * the same value stays shared and is bound once for the whole statement.
     *
     * @param DataMap|string $row
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings
     * @param BindingsMap $params Statement params accumulated across rows (extended in place)
     * @return string The compiled row WHERE fragment
     */
    private function compileBatchRowWhere(string|array $row, int|string|array|null $bindings, int $i, array &$params): string
    {
        $rowBindings = is_array($bindings) && array_is_list($bindings) ? ($bindings[$i] ?? null) : $bindings;
        [$where, $whereParams] = $this->buildWhereQuery($row, $rowBindings, "b{$i}_w_");
        [$where, $whereParams] = CommonModelPicoPdoUtils::isolateNamedPlaceholders(
            $where,
            $whereParams,
            "b{$i}_n_",
            $params
        );
        $params += $whereParams;

        return $where;
    }


    /**
     * Compiles parallel update rows into [SET clause, WHERE clause, params] for batch {@see update()}.
     *
     * Each touched column becomes one `CASE WHEN <row condition> THEN <value> … ELSE column END`,
     * and the statement WHERE is the OR of every row condition:
     * ```
     * buildUpdateSqlParts([['name' => 'A'], ['name' => 'B']], [['id' => 1], ['id' => 2]], null);
     * // SET:   name = CASE WHEN id = :b0_w_id THEN :b0_s0_name WHEN id = :b1_w_id THEN :b1_s0_name ELSE name END
     * // WHERE: (id = :b0_w_id) OR (id = :b1_w_id)
     * ```
     *
     * Rows may set different column subsets; untouched columns keep their value via `CASE ... ELSE col END`.
     * Every SET entry is compiled through {@see buildSqlClause()} and every row condition through
     * {@see buildWhereQuery()}, so each row supports the full feature set (raw SQL, `?` keys,
     * named placeholders, IN expansion). Prefixes `b{row}_w_` / `b{row}_s{entry}_` keep
     * generated placeholders collision-free across rows and entries.
     *
     * @param list<DataMap> $dataRows SET maps, one per row
     * @param list<DataMap|string> $whereRows Row conditions, parallel to $dataRows
     * @param int|string|BindingsMap|list<int|string|BindingsMap>|null $bindings List => sliced per row; anything else is shared by all rows
     * @return array{0: string, 1: string, 2: BindingsMap} [SET clause, WHERE clause, params]
     */
    private function buildUpdateSqlParts(array $dataRows, array $whereRows, int|string|array|null $bindings): array
    {
        $params = [];
        $wheres = [];
        $cases = [];

        foreach ($dataRows as $i => $row) {
            $wheres[$i] = $this->compileBatchRowWhere($whereRows[$i], $bindings, $i, $params);

            $j = 0;
            foreach ($row as $key => $value) {
                // One entry at a time: a unique prefix per row+entry keeps `?` placeholders collision-free.
                [$assignment, $assignParams] = $this->buildSqlClause([$key => $value], 'b' . $i . '_s' . $j++ . '_');
                [$column, $expression] = CommonModelPicoPdoUtils::splitAssignment($assignment);
                $cases[$column][] = "WHEN {$wheres[$i]} THEN {$expression}";
                $params += $assignParams;
            }
        }

        $set = array_map(
            static fn(string $column, array $whens): string => "{$column} = CASE " . implode(' ', $whens) . " ELSE {$column} END",
            array_keys($cases),
            $cases,
        );

        return [
            implode(', ', $set),
            implode(' OR ', array_map(static fn(string $w): string => "({$w})", $wheres)),
            $params,
        ];
    }


    /**
     * Keep a multi-statement operation all-or-nothing.
     *
     * One statement is atomic by itself; several are not, so this opens a transaction whenever
     * the operation will execute more than one statement — unless the caller already runs one,
     * whose commit then governs.
     *
     * @template T
     * @param int $statementCount
     * @param callable():T $work
     * @return mixed
     * @throws Throwable
     */
    private function withAtomicStatements(int $statementCount, callable $work): mixed
    {
        $ownTransaction = $statementCount > 1 && !$this->pdo()->inTransaction();
        if ($ownTransaction) {
            $this->pdo()->beginTransaction();
        }

        try {
            $result = $work();
            if ($ownTransaction) {
                $this->pdo()->commit();
            }
            return $result;
        } catch (Throwable $e) {
            if ($ownTransaction) {
                $this->pdo()->rollBack();
            }
            throw $e;
        }
    }
}