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
        if (array_filter($params, '\is_array')) {
            [$sql, $params] = $this->buildInQuery($sql, $params);
        }

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);
            return $stmt;
        } catch (PDOException $e) {
            if (defined('LODUR_TEST_SERVER') && LODUR_TEST_SERVER == 1) {
                array_map('error_log', str_split("<br><b>{$e->getMessage()}</b><br>{$this->getPdoDebug($stmt)}", 600) ?: []);
            }
            throw $e;
        }
    }

    /**
     * @param PDOStatement|false $stmt
     * @return string
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
     * Custom WHERE clause with bindings:
     * ```
     * $db->exists('users', 'email = ? AND created_at > ?', ['user@example.com', '2024-01-01']);
     * ```
     *
     * @param string $table Table name
     * @param string|array<string, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return bool True if at least one record exists, false otherwise
     * @throws PDOException
     */
    protected function exists(string $table, string|array|null $where = null, int|string|array|null $bindings = null): bool
    {
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = empty($where) || str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = "SELECT 1 as `true` FROM {$table} {$whereStr} LIMIT 1";
        return (bool) $this->prepExec($sql, $params)->rowCount();
    }


     /**
     * Insert a new record into the table.
     * @param string $table Table name
     * @param array<string, mixed> $data Key-value pairs of column names and values
     * @param array<string, mixed>|null $options Additional options for the insert operation
     * (e.g., ['mode' => 'REPLACE'] or ['mode' => 'INSERT IGNORE'] or ['onDuplicateKeyUpdate' => ['column' => 'value']])
     * @return int|string The ID of the inserted record, 0 if failed
     * @throws PDOException
     */
    protected function insert(string $table, array $data, array|null $options = null): int|string
    {
        $config = array_merge([
            'mode'                 => 'INSERT',
            'onDuplicateKeyUpdate' => []
        ], (array)$options);

        $columns = implode('`,`', array_keys($data));
        $placeholders = implode(',', array_fill(0, count($data), '?'));
        $params = array_values($data);

        $insertMode = match (strtoupper(trim((string)$config['mode']))) {
            'REPLACE'       => 'REPLACE',
            'INSERT IGNORE' => 'INSERT IGNORE',
            default         => 'INSERT'
        };

        $sql = "{$insertMode} INTO {$table} (`{$columns}`) VALUES ({$placeholders})";

        $onDuplicateKeyUpdate = (array) $config['onDuplicateKeyUpdate'];
        if ($insertMode === 'INSERT' && !empty($onDuplicateKeyUpdate) && !array_is_list($onDuplicateKeyUpdate)) {
            $updateClause = implode(', ', array_map(static fn($key) => "`{$key}` = ?", array_keys($onDuplicateKeyUpdate)));
            $sql .= " ON DUPLICATE KEY UPDATE {$updateClause}";
            $params = array_merge($params, array_values($onDuplicateKeyUpdate));
        }

        $isSuccess = $this->prepExec($sql, $params)->rowCount() > 0;
        $lastInsertId = $this->pdo->lastInsertId() ?: 0;
        $id = $isSuccess ? $lastInsertId : 0;
        return is_numeric($id) ? (int)$id : $id;
    }


    /**
     * @param string $table
     * @param array $data
     * @return int|string
     */
    protected function insertIgnore(string $table, array $data): int|string
    {
        return $this->insert($table, $data, ['mode' => 'INSERT IGNORE']);
    }

    /**
     * @param string $table
     * @param array $data
     * @return int|string
     */
    protected function insertReplace(string $table, array $data): int|string
    {
        return $this->insert($table, $data, ['mode' => 'REPLACE']);
    }

    /**
     * @param string $table
     * @param array $data
     * @param array $onDuplicateKeyUpdate
     * @return int|string
     */
    protected function insertOnDuplicateKeyUpdate(string $table, array $data, array $onDuplicateKeyUpdate): int|string
    {
        return $this->insert($table, $data, ['onDuplicateKeyUpdate' => $onDuplicateKeyUpdate]);
    }



    /**
     * Update table records by flexible WHERE conditions.
     *
     * ### Usage examples:
     * Classic key-value:
     * ```
     * $db->update('users', ['name' => 'John'], 'id', 1);
     * ```
     * Associative WHERE:
     * ```
     * $db->update('users', ['name' => 'John'], ['id' => 1, 'active' => 1]);
     * ```
     * Custom WHERE clause:
     * ```
     * $db->update('users', ['name' => 'John'], 'email = ? OR status = ?', ['john@example.com', 'active']);
     * ```
     *
     * @param string $table Table name
     * @param array<string, mixed> $data Key-value pairs of column names and values to update
     * @param string|array<string, mixed> $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function update(string $table, array $data, string|array $where, int|string|array|null $bindings = null): int
    {
        $setPairs = implode(',', array_map(static fn($key) => "`{$key}` = :set_{$key}", array_keys($data)));
        $params = array_combine(array_map(static fn($key) => ":set_{$key}", array_keys($data)), $data);

        [$whereStr, $whereParams] = $this->buildWhereQuery($where, $bindings);
        $whereStr = str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = "UPDATE {$table} SET {$setPairs} {$whereStr}";

        return $this->prepExec($sql, array_merge($params, $whereParams))->rowCount();
    }


    /**
     * Select specific columns from one record with optional WHERE conditions.
     *
     * ### Usage examples:
     * Classic key-value:
     * ```
     * $db->select('users', ['name', 'email'], 'id', 1);
     * ```
     * Associative array:
     * ```
     * $db->select('users', ['name'], ['status' => 'active', 'role' => 'admin']);
     * ```
     * Custom WHERE clause with bindings:
     * ```
     * $db->select('users', '*', 'status = ? AND created_at > ?', ['active', '2024-01-01']);
     * ```
     *
     * @param string $table Table name
     * @param array<string>|string|null $columns Columns to select (default '*')
     * @param string|array<string, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return array<string, mixed> Associative row or empty array if not found
     * @throws PDOException
     */
    protected function select(string $table, array|string|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null): array
    {
        $columnList = implode(', ', is_array($columns) ? $columns : [$columns ?: '*']);
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = empty($where) || str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = "SELECT {$columnList} FROM {$table} {$whereStr} LIMIT 1";
        return $this->prepExec($sql, $params)->fetch(PDO::FETCH_ASSOC) ?: [];
    }

    /**
     * Select all records from a table with optional WHERE conditions.
     *
     * ### Usage examples:
     * Classic key-value:
     * ```
     * $db->selectAll('users', '*', 'name', 'John');
     * ```
     * Without filter:
     * ```
     * $db->selectAll('users');
     * ```
     * Associative array:
     * ```
     * $db->selectAll('users', '*', ['status' => 'active', 'role' => 'admin']);
     * ```
     * Custom WHERE clause with bindings:
     * ```
     * $db->selectAll('users', '*', 'status = ? AND created_at > ?', ['active', '2024-01-01']);
     * ```
     *
     * @param string $table Table name
     * @param array<string>|string|null $columns Columns to select (default '*')
     * @param string|array<string, mixed>|null $where Column name, condition string, or associative array
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return array<int, array<string, mixed>> List of rows as associative arrays
     * @throws PDOException
     */
    protected function selectAll(string $table, array|string|null $columns = null, string|array|null $where = null, int|string|array|null $bindings = null): array
    {
        $columnList = implode(', ', is_array($columns) ? $columns : [$columns ?: '*']);
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = empty($where) || str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = trim("SELECT {$columnList} FROM {$table} {$whereStr}");

        return $this->prepExec($sql, $params)->fetchAll(PDO::FETCH_ASSOC);
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
     * Custom WHERE clause with bindings:
     * ```
     * $db->delete('users', 'last_login < ? AND status != ?', ['2023-01-01', 'active']);
     * ```
     *
     * @param string $table Table name
     * @param string|array<string, mixed> $where Column name, condition string, or associative array of conditions
     * @param int|string|array<string|int>|null $bindings Value for single column or array of bound values for custom condition
     * @return int Number of affected rows
     * @throws PDOException
     */
    protected function delete(string $table, string|array $where, int|string|array|null $bindings = null): int
    {
        [$whereStr, $params] = $this->buildWhereQuery($where, $bindings);
        $whereStr = str_contains($whereStr, 'WHERE ') ? $whereStr : 'WHERE ' . $whereStr;
        $sql = "DELETE FROM `{$table}` {$whereStr}";
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
     * @return array{string, array<string, mixed>} The modified SQL query and the updated bind parameters.
     */
    protected function buildInQuery(string $sql, array $params): array
    {
        if (empty($sql) || empty($params)) {
            return [$sql, $params];
        }

        $expandedParams = [];

        foreach ($params as $key => $value) {
            if (is_array($value) && !is_numeric($key) && !empty($value)) {

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
     * 2. Multiple key-value conditions
     * ```
     * buildWhereQuery(['id' => 1, 'status' => 'active']);
     * // `id` = :where_id AND `status` = :where_status
     * // [':where_id' => 1, ':where_status' => 'active']
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
     * @return array{string, array<string, mixed>} The WHERE clause and parameter bindings
     */
    protected function buildWhereQuery(string|array|null $where = null, int|string|array|null $bindings = null): array
    {
        if (empty($where)) {
            return ['', $bindings === null ? [] : (is_array($bindings) ? $bindings : [$bindings])];
        }

        if (is_array($where)) {
            $wherePairs = implode(' AND ', array_map(static fn($key) => "`{$key}` = :where_{$key}", array_keys($where)));
            $params = array_combine(array_map(static fn($key) => ":where_{$key}", array_keys($where)), $where);
            return [$wherePairs, is_array($bindings) ? [...$bindings, ...$params] : $params];
        } // is_string

        // For raw WHERE conditions without bindings
        if ($bindings === null) {
            return [$where, []];
        }

        // Handle placeholders in WHERE clause
        if (is_array($bindings) && !empty($bindings)) {
            if (array_filter($bindings, '\is_array')) {
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
        return ["`{$where}` = :where_{$where}", [":where_{$where}" => $bindings]];
    }
}