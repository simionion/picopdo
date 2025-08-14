<?php
declare(strict_types=1);

namespace Lodur\PicoPdo;

use PDO;
use PDOStatement;
use PDOException;




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
 * - Requires a `$pdo` property of type PDO to be defined in the consuming class.
 * - Provides protected helper methods for common database tasks.
 *
 * ## Features:
 * - Secure, parameterized queries via `prepExec()`, with array-expansion for `IN` clauses.
 * - Simplified methods for SELECT, INSERT, UPDATE, DELETE, and EXISTS operations.
 * - Automatically binds parameters and handles execution errors.
 * - Reduces duplication and boilerplate in model implementations.
 */
trait CommonModelPicoPdoTrait
{
    protected PDO $pdo;

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
     * // Params: ["ids0" => 1, "ids1" => 2, "ids2" => 3]
     * ```
     *
     * @param string $sql The SQL query.
     * @param array<int|string, mixed> $params The parameters to bind.
     * @return PDOStatement The executed statement.
     * @throws PDOException If the query fails to execute.
     */
    protected function prepExec(string $sql, array $params = []): PDOStatement
    {
        if (array_filter($params, is_array(...))) {
            [$sql, $params] = $this->buildInQuery($sql, $params);
        }

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);
            return $stmt;
        } catch (PDOException $e) {
            if (defined('LODUR_TEST_SERVER') && LODUR_TEST_SERVER == 1) {
                array_map(error_log(...), str_split("<br><b>{$e->getMessage()}</b><br>{$this->getPdoDebug($stmt)}", 600) ?: []);
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
     * ### Usage examples:
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
     * Multiple values for WHERE IN (named placeholders only)
     * ```
     * $db->exists('users', 'id IN (:ids)', [':ids' => [1, 2, 3]]);
     * ```
     *
     * @param string $table Table name
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return bool True if at least one record exists, false otherwise
     * @throws PDOException
     */
    protected function exists(string $table, string|array|null $where = null, int|string|array|null $bindings = null): bool
    {
        return (bool)$this->select($table, '1 as `true`', $where, $bindings, 'LIMIT 1')->fetchColumn();
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
     * ```
     * @param string $table Table name
     * @param array<string|int, mixed> $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @param array<string, mixed>|null $options Additional options for the insert operation
     * (e.g., ['mode' => 'REPLACE'] or ['mode' => 'INSERT IGNORE'] or ['onDuplicateKeyUpdate' => ['column' => 'value']])
     * @return int|string|array<string, mixed> The ID of the inserted record, 0 if failed. If 'meta' is set to true, returns an array with meta info like ['id', 'rows', 'status' => 'noop|inserted|updated']
     * @throws PDOException
     */
    protected function insert(string $table, array $data, array|null $options = null): int|string|array
    {
        $config = array_merge([
            'mode'                 => 'INSERT',
            'meta'                 => false, // By default returns the ID else 0. Set `true` to return ['id' => $id, 'rows' => $affectedRows, 'status' => 'noop|inserted|updated']
            'onDuplicateKeyUpdate' => []
        ], (array)$options);

        $onDuplicateKeyUpdate = (array)$config['onDuplicateKeyUpdate'];

        $insertMode = match (strtoupper(trim((string)$config['mode']))) {
            'REPLACE'       => 'REPLACE', /*insertReplace(...)*/
            'INSERT IGNORE' => 'INSERT IGNORE', /*insertIgnore(...)*/
            default         => 'INSERT'
        };

        [$setClause, $params] = $this->buildSqlClause($data, 'set_');
        $sql = "{$insertMode} INTO {$table} SET {$setClause}";

        if ($insertMode === 'INSERT' && !empty($onDuplicateKeyUpdate)) {
            [$updateClause, $updateParams] = $this->buildSqlClause($onDuplicateKeyUpdate, 'upd_');
            $params = array_merge($params, $updateParams);
            $sql .= " ON DUPLICATE KEY UPDATE {$updateClause}";
        }

        $rowCount = $this->prepExec($sql, $params)->rowCount();
        $isSuccess =  $rowCount > 0;
        $lastInsertId = $this->pdo->lastInsertId() ?: 0;
        $rawId = $isSuccess ? $lastInsertId : 0;
        $id = is_numeric($rawId) ? (int)$rawId : $rawId;
        if($config['meta']) {
            return [
                'id' => $id,
                'rows' => $rowCount,
                'status' => match ($rowCount) {
                    0       => 'noop',
                    1       => 'inserted',
                    default => 'updated'
                }
            ];
        }

        return $id;
    }


    /**
     * This is a wrapper around {@see insert()} that performs a `REPLACE INTO`.
     * @param string $table Table name
     * @param array<string|int, mixed> $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
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
     * @param array<string|int, mixed> $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @return array<string, mixed> Inserted record meta info ['id', 'rows', 'status' => 'noop|inserted|updated']
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
     * @param array<string|int, mixed> $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @param array<string|int, mixed> $onDuplicateKeyUpdate Key-value pairs of column names and values to update on duplicate key or raw sql queries like 'date = NOW()'
     * @return array<string, mixed> Inserted record meta info ['id', 'rows', 'status' => 'noop|inserted|updated']
     */
    protected function insertOnDuplicateKeyUpdate(string $table, array $data, array $onDuplicateKeyUpdate): array
    {
        return $this->insert($table, $data, ['onDuplicateKeyUpdate' => $onDuplicateKeyUpdate, 'meta' => true]);
    }


    /**
     * Update table records by flexible WHERE conditions.
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
     * Multiple values for WHERE IN (named placeholders only)
     * ```
     * $db->update('users', ['name' => 'John'], 'id IN (:ids)', [':ids' => [1, 2, 3]]);
     * ```
     *
     * @param string $table Table name
     * @param array<string|int, mixed> $data Key-value pairs of column names and values to update or raw sql queries like 'date = NOW()'
     * @param string|array<string|int, mixed> $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function update(string $table, array $data, string|array $where, int|string|array|null $bindings = null): int
    {
        [$setClause, $params] = $this->buildSqlClause($data, 'set_', ', ');
        [$whereStr, $whereParams] = $this->buildWhereQuery($where, $bindings);
        $whereStr = str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = "UPDATE {$table} SET {$setClause} {$whereStr}";

        return $this->prepExec($sql, array_merge($params, $whereParams))->rowCount();
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
     * Multiple values for WHERE IN (named placeholders only)
     * ```
     * $db->select('users', 'id, name', 'id IN (:ids)', [':ids' => [1, 2, 3]])->fetch(PDO::FETCH_ASSOC);
     * ```
     *
     * @param string $table Table name
     * @param array<string>|string|null $columns Columns to select (default '*')
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return PDOStatement|false
     */
    protected function select(string $table, array|string|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): PDOStatement|false
    {
        $columnList = implode(', ', is_array($columns) ? $columns : [$columns ?: '*']);
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = empty($where) || str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = "SELECT {$columnList} FROM {$table} {$whereStr} {$extraQuerySuffix}";
        return $this->prepExec($sql, $params);
    }


    /**
     * Wrapper for {@see select()} to fetch one row only, LIMIT 1 is appended automatically.
     *
     * @param string $table Table name
     * @param array<string>|string|null $columns Columns to select (default '*')
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY .. ) LIMIT 1 is appended automatically.
     * @return array
     */
    protected function selectOne(string $table, array|string|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): array
    {
        $stmt = $this->select($table, $columns, $where, $bindings, $extraQuerySuffix . ' LIMIT 1');
        if (!$stmt) {
            return [];
        }
        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        return $result ?: [];
    }

    /**
     * Wrapper for {@see select()} to fetch all rows.
     *
     * @param string $table Table name
     * @param array<string>|string|null $columns Columns to select (default '*')
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return array
     */
    protected function selectAll(string $table, array|string|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): array
    {
        $stmt = $this->select($table, $columns, $where, $bindings, $extraQuerySuffix);
        if (!$stmt) {
            return [];
        }
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        return $result ?: [];
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
     *
     * @param string $table Table name
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function delete(string $table, string|array $where, int|string|array|null $bindings = null): int
    {
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = "DELETE FROM {$table} {$whereStr}";
        return $this->prepExec($sql, $params)->rowCount();
    }


    /**
     * Expands named placeholders in SQL queries for `IN` clauses.
     *
     * - Replaces named placeholders (`:key`) with multiple placeholders (`:key0, :key1, :key2`) for `WHERE IN` queries.
     * - Updates the bind parameters to match the expanded placeholders.
     * - Supports **only named placeholders** for arrays (positional `?` placeholders are not expanded).
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
     * @param array<string, mixed> $params The parameters to bind, where array values are expanded.
     * @return array{0: string, 1: array<string, mixed>} The modified SQL query and the updated bind parameters.
     */
    protected function buildInQuery(string $sql, array $params): array
    {
        if (empty($sql) || empty($params)) {
            return [$sql, $params];
        }

        $expandedParams = [];

        foreach ($params as $key => $value) {
            if (is_array($value)) {

                if(is_numeric($key) || empty($value)){
                    if(defined('LODUR_TEST_SERVER') && LODUR_TEST_SERVER == 1) {
                        error_log('Provided array for IN clause is empty or key is numeric - ' . $sql . ' - ' . json_encode([$key => $value]));
                    }
                    unset($params[(string)$key]);
                    continue;
                }

                // Only support named parameters for array values
                $searchKey = str_starts_with($key, ':') ? $key : ":{$key}";

                $valuesIn = array_values($value); // Ensure values are indexed

                $placeholders = array_map(static fn($index) => "{$searchKey}{$index}", array_keys($valuesIn));

                // Match :key followed by a word boundary to prevent partial matches
                // This ensures :ids doesn't match :ids_extra or :ids2
                $pattern = '/(?<!:)' . preg_quote($searchKey, '/') . '\b/';

                $sql = preg_replace($pattern, implode(',', $placeholders), $sql);

                $expandedParams = [...$expandedParams, ...array_combine($placeholders, $valuesIn)];

                // Remove original array key
                unset($params[$key]);
            } else {
                $expandedParams[(string)$key] = $value;
            }
        }

        return [$sql, $expandedParams];
    }


    /**
     * Build SQL clauses and parameters from mixed data array
     * ```
     * buildSqlClause(['name' => 'John', 'email' => 'john@example.com', 'created_at = NOW()'], 'insert_');
     * //sql: `name` = :insert_name, `email` = :insert_email, created_at = NOW()
     * //params: [':insert_name' => 'John', ':insert_email' => 'john@example.com']
     * ```
     * With AND joiner
     * ```
     * buildSqlClause(['name' => 'John', 'email' => 'john@example.com', 'created_at = NOW()'], 'insert_', ' AND ');
     * //sql: `name` = :insert_name AND `email` = :insert_email AND created_at = NOW()
     * //params: [':insert_name' => 'John', ':insert_email' => 'john@example.com']
     * ```
     *
     * @param array<int|string, mixed> $data Mixed array of key-value pairs and raw SQL strings
     * @param string|null $prefix Parameter prefix (e.g., 'set_', 'insert_', 'update_')
     * @param string $joiner How to join the clauses (e.g., ', ', ' AND ', ' OR ')
     * @return array{0: string, 1: array<string, mixed>} [sqlClause, parameters]
     */
    protected function buildSqlClause(array $data, string|null $prefix = null, string $joiner = ', '): array
    {
        $sqlPairs = [];
        $params = [];

        foreach ($data as $key => $value) {
            if (is_numeric($key)) {
                // Raw SQL like "updated_at = NOW()"
                $sqlPairs[] = $value;
            } else {
                // Key-value pair like "name" => "John"
                $sqlPairs[] = "`{$key}` = :{$prefix}{$key}";
                $params[":{$prefix}{$key}"] = $value;
            }
        }

        return [implode($joiner, $sqlPairs), $params];
    }

    /**
     * Builds a SQL `WHERE` clause and its corresponding named parameter bindings
     * from flexible inputs.
     *
     * Supports:
     * - Associative arrays for simple equality comparisons
     * - Raw condition strings with:
     * - `?` placeholders (auto-converted to named `:where_0`, `:where_1`, etc.)
     * - Named placeholders (e.g. `:role`, `:status`, `:ids`) used as-is or expanded
     * - Single column/value shorthand
     * - Raw SQL condition without bindings
     * - Automatic expansion of arrays for `IN (:placeholder)` patterns
     *
     * For UPDATE/DELETE operations, if no WHERE clause is provided,
     * an invalid `WHERE` clause (`WHERE `) will be returned, triggering a PDO exception
     * to prevent accidental full-table operations.
     *
     * ### Examples (increasing complexity):
     *
     * 1. Single key-value pair
     * ```
     * buildWhereQuery('id', 1);
     * // `id` = :where_id
     * // [':where_id' => 1]
     * ```
     *
     * 2.a Multiple key-value conditions
     * ```
     * buildWhereQuery(['id' => 1, 'status' => 'active']);
     * // `id` = :where_id AND `status` = :where_status
     * // [':where_id' => 1, ':where_status' => 'active']
     * ```
     *
     * 2.b Multiple key-value conditions with raw SQL as keyless entries
     * ```
     * buildWhereQuery(['id' => 1, 'status' => 'active', 'email_verified != 0', 'created_at > :date'], [':date' => $date]);
     * // `id` = :where_id AND `status` = :where_status AND email_verified != 0 AND created_at > :date
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
     * @param string|array<string, mixed> $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return array{0: string, 1: array<string, mixed>} The WHERE clause and parameter bindings
     */
    protected function buildWhereQuery(string|array|null $where = null, int|string|array|null $bindings = null): array
    {
        if (empty($where)) {
            return ['', $bindings === null ? [] : (is_array($bindings) ? $bindings : [$bindings])];
        }

        if (is_array($where)) {
            [$whereClause, $params] = $this->buildSqlClause($where, 'where_', ' AND ');
            return [$whereClause, $bindings === null ? $params : [...((array)$bindings), ...$params]] ;
        } // is_string

        // For raw WHERE conditions without bindings
        if ($bindings === null) {
            return [$where, []];
        }

        // Handle placeholders in WHERE clause
        if (is_array($bindings) && !empty($bindings)) {
            if (array_filter($bindings, is_array(...))) {
                [$where, $bindings] = $this->buildInQuery($where, $bindings);
            }

            // If we have named placeholders, use them directly
            if (str_contains($where, ':')) {
                return [$where, $bindings];
            }

            // If we have ? placeholders, convert them to named parameters
            if (str_contains($where, '?')) {

                $i = 0;
                $whereClause = preg_replace_callback('/\?/', static function () use (&$i, &$bindings) {
                    $param = ":where_{$i}";
                    if (array_key_exists($i, $bindings)) { // Let PDO handle missing bindings
                        $bindings[$param] = $bindings[$i];
                        unset($bindings[$i]); // Remove the used binding
                    }
                    $i++;
                    return $param;
                }, $where);

                return [$whereClause, $bindings];
            }
        }

        // Binding with scalar value
        if (str_contains($where, ':') || str_contains($where, '?')) {
            return [$where, is_array($bindings) ? $bindings : [$bindings]];
        }

        // For single column = value condition
        return ["{$where} = :where_{$where}", [":where_{$where}" => $bindings]];
    }



    /**
     * Returns the list of columns for a given table
     *
     * @param string $table The table name
     * @return array<int, string> Array of column names
     */
    protected function getTableColumns(string $table): array
    {
        $table = strtolower($this->normalizeTableName($table));
        $sql = "DESCRIBE `{$table}`";
        $key = CommonCache::createKey($sql);
        if (is_array($cols = CommonCache::get($key))) {
            return $cols;
        }

        $cols = array_column($this->prepExec($sql)->fetchAll(PDO::FETCH_ASSOC), 'Field');
        CommonCache::set($key, $cols);
        return $cols;
    }


    /**
     * Filter out invalid columns from a list of columns or assoc array with columns as keys.
     * ```
     * $table = 'users';
     * $columns = ['id', 'name', 'email', 'invalid_column'];
     * $validColumns = $db->removeInvalidColumns($table, $columns);
     * // $validColumns will be ['id', 'name', 'email']
     *
     * $columns = ['id' => 1, 'name' => 'John', 'email' => 'john@example.com', 'invalid_column' => 'invalid'];
     * $validColumns = $db->removeInvalidColumns($table, $columns);
     * // $validColumns will be ['id' => 1, 'name' => 'John', 'email' => 'john@example.com']
     * ```
     *
     * @param string $table The table name
     * @param array<int|string, mixed> $columns Array of column names or associative array with column names as keys
     * @return array<int|string, mixed> Filtered array with only valid columns
     */
    protected function removeInvalidColumns(string $table, array $columns): array
    {
        $isList = array_is_list($columns);
        $validColumns = array_map(strtolower(...), $this->getTableColumns($table));
        $filteredColumns = array_filter($columns, static fn($v, $k) => in_array(strtolower($isList ? $v : $k), $validColumns, true), ARRAY_FILTER_USE_BOTH);
        return $isList ? array_values($filteredColumns) : $filteredColumns;
    }


    /**
     * Normalize a table name by removing invalid characters.
     *
     * @param string $table The table name to normalize
     * @return string The normalized table name with invalid characters removed
     */
    private function normalizeTableName(string $table): string
    {
        return preg_replace('/\W/', '', $table);
    }
}