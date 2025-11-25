<?php
declare(strict_types=1);

namespace Lodur\PicoPdo;

use PDO;
use PDOException;
use PDOStatement;


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
 *
 * @update Automatic conversion of `?` placeholders to named placeholders
 * All `?` placeholders in WHERE clauses and SQL clauses are automatically converted to named placeholders
 * (e.g., `:where_0`, `:where_1`, `:set_0`, etc.) for consistency and better support (ex: usage of IN(?)  [[1,2,3]])
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
     * @param array<int|string, mixed>|string|int|null $params The parameters to bind. Can be scalar
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
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);
            return $stmt;
        } catch (PDOException $e) {
            if (defined('LODUR_TEST_SERVER')) {
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
     *  'create_at > ?' => $date // new* - direct binding for ? placeholder
     * ],
     *  bindings:[':date' => $date]);
     * ```
     * @param string $table Table name
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int, mixed>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return bool True if at least one record exists, false otherwise
     * @throws PDOException
     */
    protected function exists(string $table, string|array|null $where = null, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): bool
    {
        return (bool)$this->select($table, '1 as `true`', $where, $bindings, trim("{$extraQuerySuffix} LIMIT 1"))->fetchColumn();
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

        [$setClause, $params] = $this->buildSqlClause($data, 'set_', ', ');
        $sql = "{$insertMode} INTO {$table} SET {$setClause}";

        if ($insertMode === 'INSERT' && !empty($onDuplicateKeyUpdate)) {
            [$updateClause, $params] = $this->buildSqlClause($onDuplicateKeyUpdate, 'upd_', ', ', $params);
            $sql .= " ON DUPLICATE KEY UPDATE {$updateClause}";
        }

        $rowCount = $this->prepExec($sql, $params)->rowCount();
        $isSuccess = $rowCount > 0;
        $lastInsertId = $this->pdo->lastInsertId() ?: 0;
        $rawId = $isSuccess ? $lastInsertId : 0;
        $id = is_numeric($rawId) ? (int)$rawId : $rawId;
        if ($config['meta']) {
            return [
                'id'     => $id,
                'rows'   => $rowCount,
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
     * @param array<string|int, mixed> $data Key-value pairs of column names and values or raw sql queries like 'date = NOW()'
     * @param array<string|int, mixed> $onDuplicateKeyUpdate Key-value pairs of column names and values to update on duplicate key or raw sql queries like 'date = NOW()'
     * @return array{id: int|string, rows: int, status: string} Inserted record meta info ['id', 'rows', 'status' => 'noop|inserted|updated']
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
     * Multiple values for WHERE IN with named placeholders
     * ```
     * $db->update('users', ['name' => 'John'], 'id IN (:ids)', [':ids' => [1, 2, 3]]);
     * ```
     * Multiple values for WHERE IN with `?` placeholder:
     * ```
     * $db->update('users', 'id IN (?)', [[1, 2, 3]]);
     * ```
     * Associative array with advanced bindings:
     * ```
     * $db->update('users', where:[
     *  'status' => $status, // simple column & binding value
     *  'email_verified != 0', // raw sql, no binding
     *  'created_at > :date'  // raw sql with placeholder binding (ph needs to be added in bindings)
     *  'create_at > ?' => $date // new* - direct binding for ? placeholder
     * ],
     *  bindings:[':date' => $date]);
     * ```
     *
     * @param string $table Table name
     * @param array<string|int, mixed> $data Key-value pairs of column names and values to update or raw sql queries like 'date = NOW()'
     * @param string|array<string|int, mixed> $where Column name, condition string, or associative array
     * @param int|string|array<string|int, mixed>|null $bindings Value for single column or array of bound values for custom condition
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function update(string $table, array $data, string|array $where, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): int
    {
        [$setClause, $params] = $this->buildSqlClause($data, 'set_', ', ');
        [$whereStr, $whereParams] = $this->buildWhereQuery($where, $bindings);
        $whereStr = str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = trim("UPDATE {$table} SET {$setClause} {$whereStr} {$extraQuerySuffix}");

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
     * Multiple values for WHERE IN with named placeholders
     * ```
     * $db->select('users', 'id, name', 'id IN (:ids)', [':ids' => [1, 2, 3]])->fetch(PDO::FETCH_ASSOC);
     * ```
     * Multiple values for WHERE IN with `?` placeholder:
     * ```
     * $db->select('users', 'id IN (?)', [[1, 2, 3]]);
     * ```
     * Associative array with advanced bindings:
     * ```
     * $db->select('users', where:[
     *  'status' => $status, // simple column & binding value
     *  'email_verified != 0', // raw sql, no binding
     *  'created_at > :date'  // raw sql with placeholder binding (ph needs to be added in bindings)
     *  'create_at > ?' => $date // new* - direct binding for ? placeholder
     * ],
     *  bindings:[':date' => $date])->fetchAll()
     * ```
     * @param string $table Table name
     * @param array<string>|string|int|null $columns Columns to select (default '*')
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int, mixed>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return PDOStatement|false
     */
    protected function select(string $table, array|string|int|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): PDOStatement|false
    {
        $columnList = implode(', ', is_array($columns) ? $columns : [$columns ?: '*']);
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = empty($where) || str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = trim("SELECT {$columnList} FROM {$table} {$whereStr} {$extraQuerySuffix}");
        return $this->prepExec($sql, $params);
    }


    /**
     * Wrapper for {@see select()} to fetch one row only, LIMIT 1 is appended automatically.
     *
     * @param string $table Table name
     * @param array<string>|string|int|null $columns Columns to select (default '*')
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int, mixed>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY .. ) LIMIT 1 is appended automatically.
     * @return array<string, mixed>
     */
    protected function selectOne(string $table, array|string|int|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): array
    {
        return $this->select($table, $columns, $where, $bindings, trim("{$extraQuerySuffix} LIMIT 1"))->fetch(PDO::FETCH_ASSOC) ?: [];
    }

    /**
     * Wrapper for {@see select()} to fetch all rows.
     *
     * @param string $table Table name
     * @param array<string>|string|int|null $columns Columns to select (default '*')
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int, mixed>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return array<int, array<string, mixed>>
     */
    protected function selectAll(string $table, array|string|int|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): array
    {
        return $this->select($table, $columns, $where, $bindings, $extraQuerySuffix)->fetchAll(PDO::FETCH_ASSOC);
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
     * @param string|array<string|int, mixed> $where Column name, condition string, or associative array
     * @param int|string|array<string|int, mixed>|null $bindings Value for single column or array of bound values for custom condition
     * @param string|null $extraQuerySuffix Extra query suffix (e.g. GROUP BY, ORDER BY, LIMIT, etc.)
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function delete(string $table, string|array $where, int|string|array|null $bindings = null, string|null $extraQuerySuffix = null): int
    {
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = trim("DELETE FROM {$table} {$whereStr} {$extraQuerySuffix}");
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
                    if(defined('LODUR_TEST_SERVER')) {
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
     * Build SQL clauses and parameters from mixed data array, converting to named placeholders for key-value pairs.
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
     * With ? placeholders converted to named:
     * ```
     * buildSqlClause(['name' => 'John', 'email' => 'john@example,com', 'last_login = ?' => '2024-01-01'], 'update_', ', ');
     * //sql: `name` = :update_name, `email` = :update_email, last_login = :update_0
     * //params: [':update_name' => 'John', ':update_email' => 'john@example,com', ':update_0' => '2024-01-01']
     *
     * @param array<int|string, mixed> $data Mixed array of key-value pairs and raw SQL strings
     * @param string|null $prefix Parameter prefix (e.g., 'set_', 'insert_', 'update_')
     * @param string|null $joiner How to join the clauses (e.g., ', ', ' AND ', ' OR ')
     * @param array $bindings - Additional bindings for raw SQL entries not listed in $data, pass through in mind.
     * @return array{0: string, 1: array<string, mixed>} [sqlClause, parameters]
     */
    protected function buildSqlClause(array $data, string|null $prefix = null, string|null $joiner = null, array $bindings = []): array
    {
        $joiner ??= ', ';
        $sqlPairs = [];
        $sqlRaws = [];
        $params = [];

        foreach ($data as $key => $value) {
            if (is_numeric($key)) {
                // Raw SQL like "updated_at = NOW()" or "date > ?"
                $sqlRaws[] = $value;
            } elseif (str_contains($key, '?')) {
                // Key with ? placeholder like ['last_login > ?' => $date]
                $sqlPairs[] = $key;
                $params[] = $value;
            } else {
                // Key-value pair like "name" => "John"
                $sqlPairs[] = "`{$key}` = :{$prefix}{$key}";
                $params[":{$prefix}{$key}"] = $value;
            }
        }

        [$pairsSql, $pairsParams] = $this->convertToNamedPlaceholders(implode($joiner, $sqlPairs), $params, $prefix);
        [$rawsSql, $rawsParams] = $this->convertToNamedPlaceholders(implode($joiner, $sqlRaws), $bindings, "{$prefix}raw_");

        return [implode($joiner, array_filter([$pairsSql, $rawsSql])), array_merge($pairsParams, $rawsParams)];
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
     * 2.b Multiple key-value conditions with raw SQL as numeric key entries
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
     * ```
     * 7. Raw SQL with direct bindings
     * ```
     * buildWhereQuery(['id IN (?)' => $ids, 'created_at > ?' => $date]);
     *
     * @param string|array<string|int, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int, mixed>|null $bindings Value for single column or array of bound values for custom condition
     * @return array{0: string, 1: array<string, mixed>} The WHERE clause and parameter bindings
     */
    protected function buildWhereQuery(string|array|null $where = null, int|string|array|null $bindings = null): array
    {
        if (empty($where)) {
            return ['', (array)$bindings];
        }

        if (is_array($where)) {
            [$where, $bindings] = $this->buildSqlClause($where, 'where_', ' AND ', (array)$bindings);
        }

        if (str_contains($where, '?')) {
            [$where, $bindings] = $this->convertToNamedPlaceholders($where, (array)$bindings, 'where_');
        }

        if (is_array($bindings) && array_filter($bindings, is_array(...))) {
            [$where, $bindings] = $this->buildInQuery($where, $bindings);
        }

        if (str_contains($where, ':')) {
            return [$where, (array)$bindings];
        }

        if (is_scalar($bindings)) {
            return ["{$where} = :where_{$where}", [":where_{$where}" => $bindings]];
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
     * @param mixed $bindings
     * @param string|null $prefix
     * @return array
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

}