<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use ValueError;

/**
 * Unit tests without PHPUnit mocks: pure SQL helpers are asserted as strings; anything that runs SQL uses the same
 * MySQL instance as integration ({@see phpunit.xml} <env DB_* />), via dedicated tables so we do not touch `test_*`.
 */
class CommonModelPicoPdoTraitTest extends TestCase
{
    private const TABLE_USERS = 'unit_trait_users';

    private const TABLE_PROFILES = 'unit_trait_profiles';

    use CommonModelPicoPdoTrait {
        prepExec as public _testPrepExec;
        buildWhereQuery as public _testBuildWhereQuery;
        exists as public _testExists;
        buildInQuery as public _testBuildInQuery;
        buildSqlClause as public _testBuildSqlClause;
        convertToNamedPlaceholders as public _testConvertToNamedPlaceholders;
        getPdoDebug as public _testGetPdoDebug;
    }

    protected PDO $pdo;

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo = new PDO(
            'mysql:host=' . getenv('DB_HOST') . ';dbname=' . getenv('DB_NAME'),
            getenv('DB_USER'),
            getenv('DB_PASS'),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE_PROFILES);
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE_USERS);
        $this->pdo->exec(
            'CREATE TABLE ' . self::TABLE_USERS . ' (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NULL,
                email VARCHAR(255) NULL,
                status VARCHAR(64) NULL,
                email_verified INT DEFAULT 0,
                created_at DATETIME NULL
            )'
        );
        $this->pdo->exec(
            'CREATE TABLE ' . self::TABLE_PROFILES . ' (
                user_id INT NOT NULL,
                bio TEXT
            )'
        );
    }

    // ——— prepExec (real PDO) ———

    public function testPrepExecWithSimplePositionalPlaceholder(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name) VALUES (5, 'x')");
        $stmt = $this->_testPrepExec("SELECT name FROM {$t} WHERE id = ?", [5]);
        $this->assertSame('x', $stmt->fetchColumn());
    }

    public function testPrepExecWithNamedPlaceholders(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name) VALUES (10, 'n')");
        $stmt = $this->_testPrepExec("SELECT name FROM {$t} WHERE id = :id", ['id' => 10]);
        $this->assertSame('n', $stmt->fetchColumn());
    }

    public function testPrepExecWithInClauseExpansion(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, status) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        $stmt = $this->_testPrepExec(
            "SELECT id FROM {$t} WHERE status IN (:st)",
            [':st' => ['a', 'b']]
        );
        $ids = $stmt->fetchAll(PDO::FETCH_COLUMN);
        sort($ids);
        $this->assertSame([1, 2], $ids);
    }

    public function testPrepExecWithTwoInClauses(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, status, name) VALUES (1, 'active', 'admin'), (2, 'pending', 'user')");
        $stmt = $this->_testPrepExec(
            "SELECT id FROM {$t} WHERE status IN (:statuses) AND name IN (:roles)",
            [
                ':statuses' => ['active', 'pending'],
                ':roles' => ['admin', 'user'],
            ]
        );
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    public function testPrepExecWithEmptySql(): void
    {
        // PHP 8.4+ ValueError on empty prepare(); 8.2 may still accept it — accept either outcome.
        try {
            $stmt = $this->_testPrepExec('', []);
            $this->assertInstanceOf(PDOStatement::class, $stmt);
        } catch (ValueError) {
            $this->addToAssertionCount(1);
        }
    }

    public function testPrepExecWithFailedExecution(): void
    {
        $this->expectException(PDOException::class);
        $this->_testPrepExec('SELECT * FROM no_such_table WHERE id = :id', ['id' => 1]);
    }

    public function testPrepExecWithInvalidSqlThrows(): void
    {
        $this->expectException(PDOException::class);
        $this->_testPrepExec('NOT VALID SQL SYNTAX [[[', []);
    }

    // ——— exists (real PDO) ———

    public function testExistsWithKeyValue(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, email) VALUES (1, 'e@e')");
        $this->assertTrue($this->_testExists($t, 'id', 1));
    }

    public function testExistsWithAssociativeArray(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (status, email_verified) VALUES ('active', 1)");
        $this->assertTrue($this->_testExists($t, ['status' => 'active', 'email_verified' => 1]));
    }

    public function testExistsWithCustomWhereClause(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (email, created_at) VALUES ('user@example.com', '2024-01-02')");
        $this->assertTrue($this->_testExists(
            $t,
            'email = ? AND created_at > ?',
            ['user@example.com', '2024-01-01']
        ));
    }

    public function testExistsWithNullWhere(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id) VALUES (1)");
        $this->assertTrue($this->_testExists($t, null));
    }

    public function testExistsWithEmptyArrayWhere(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id) VALUES (1)");
        $this->assertTrue($this->_testExists($t, []));
    }

    // ——— getPdoDebug ———

    public function testGetPdoDebugWithFalseStatement(): void
    {
        $this->assertSame('Statement preparation failed', $this->_testGetPdoDebug(false));
    }

    public function testGetPdoDebugWithRealStatement(): void
    {
        $stmt = $this->pdo->query('SELECT 1 AS x');
        $this->assertIsString($this->_testGetPdoDebug($stmt));
    }

    // ——— buildInQuery ———

    public function testBuildInQueryWithNonArrayValues(): void
    {
        $sql = 'SELECT * FROM users WHERE id = :id AND status = :status';
        $params = [':id' => 1, ':status' => 'active'];
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        $this->assertSame($sql, $newSql);
        $this->assertSame($params, $newParams);
    }

    public function testBuildInQuery(): void
    {
        $sql = 'SELECT * FROM users WHERE id IN (:ids)';
        $params = [':ids' => [1, 2, 3]];
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        $this->assertSame('SELECT * FROM users WHERE id IN (:ids0,:ids1,:ids2)', $newSql);
        $this->assertSame([':ids0' => 1, ':ids1' => 2, ':ids2' => 3], $newParams);
    }

    public function testBuildInQueryWithEmptyInputs(): void
    {
        [$newSql, $newParams] = $this->_testBuildInQuery('', []);
        $this->assertSame('', $newSql);
        $this->assertSame([], $newParams);
    }

    public function testBuildInQueryWithNumericKeys(): void
    {
        $sql = 'SELECT * FROM users WHERE id IN (?)';
        $params = [0 => [1, 2, 3]];
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        $this->assertSame($sql, $newSql);
        $this->assertSame([], $newParams);
    }

    public function testBuildInQueryWithArrayParameters(): void
    {
        $sql = 'SELECT * FROM users WHERE id IN (:ids)';
        $params = [':ids' => [1, 2, 3]];
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        $this->assertSame('SELECT * FROM users WHERE id IN (:ids0,:ids1,:ids2)', $newSql);
        $this->assertSame([':ids0' => 1, ':ids1' => 2, ':ids2' => 3], $newParams);
    }

    public function testBuildInQueryWithMultipleArraysInOneQuery(): void
    {
        $sql = 'SELECT * FROM users WHERE id IN (:ids) AND status IN (:statuses)';
        $params = [
            ':ids' => [1, 2, 3],
            ':statuses' => ['active', 'pending'],
        ];
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        $this->assertSame(
            'SELECT * FROM users WHERE id IN (:ids0,:ids1,:ids2) AND status IN (:statuses0,:statuses1)',
            $newSql
        );
        $this->assertSame([
            ':ids0' => 1,
            ':ids1' => 2,
            ':ids2' => 3,
            ':statuses0' => 'active',
            ':statuses1' => 'pending',
        ], $newParams);
    }

    /**
     * LODUR_TEST_SERVER enables extra error_log calls; redirect PHP's error_log to a temp file so PHPUnit does not treat
     * log output as a test failure, while still executing those lines for coverage.
     *
     * @runInSeparateProcess
     * @preserveGlobalState disabled
     */
    public function testLodurTestServerDebugBranchesWithErrorLogRedirect(): void
    {
        $logFile = sys_get_temp_dir() . '/picopdo_phpunit_' . uniqid('', true) . '.log';
        $previousErrorLog = ini_set('error_log', $logFile);
        try {
            define('LODUR_TEST_SERVER', true);

            $sql = 'SELECT * FROM users WHERE id IN (:ids)';
            [$newSql, $newParams] = $this->_testBuildInQuery($sql, [':ids' => []]);
            $this->assertSame($sql, $newSql);
            $this->assertSame([], $newParams);

            try {
                $this->_testPrepExec('SELECT * FROM ' . self::TABLE_USERS . '_definitely_missing', []);
                $this->fail('Expected PDOException');
            } catch (PDOException $e) {
                $this->assertStringContainsString("doesn't exist", $e->getMessage());
            }
        } finally {
            if ($previousErrorLog !== false) {
                ini_set('error_log', $previousErrorLog);
            }
            if (is_file($logFile)) {
                unlink($logFile);
            }
        }
    }

    // ——— buildWhereQuery ———

    public function testBuildWhereQueryWithArray(): void
    {
        $where = ['id' => 1, 'name' => 'John'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);
        $this->assertSame('id = :where_id AND name = :where_name', $whereStr);
        $this->assertSame([':where_id' => 1, ':where_name' => 'John'], $params);
    }

    public function testBuildWhereQueryWithString(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('id', 1);
        $this->assertSame('id = :where_id', $whereStr);
        $this->assertSame([':where_id' => 1], $params);
    }

    /**
     * Column name + array bindings is returned as-is (not coerced to `col = :where_col`); use a scalar binding or a full condition string instead.
     */
    public function testBuildWhereQueryWithStringAndArrayBindingsPassesThrough(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('id', [99]);
        $this->assertSame('id', $whereStr);
        $this->assertSame([99], $params);
    }

    public function testBuildWhereQueryWithCustomCondition(): void
    {
        $bindings = [':status' => 'active', ':date' => '2024-01-01'];
        [$whereStr, $params] = $this->_testBuildWhereQuery(
            'status = :status AND created_at > :date',
            $bindings
        );
        $this->assertSame('status = :status AND created_at > :date', $whereStr);
        $this->assertSame($bindings, $params);
    }

    public function testBuildWhereQueryWithNullWhere(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery(null);
        $this->assertSame('', $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithEmptyArray(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery([]);
        $this->assertSame('', $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithIsNull(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('email IS NULL');
        $this->assertSame('email IS NULL', $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithIsNotNull(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('email IS NOT NULL');
        $this->assertSame('email IS NOT NULL', $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithLike(): void
    {
        $bindings = [':name' => '%John%'];
        [$whereStr, $params] = $this->_testBuildWhereQuery('name LIKE :name', $bindings);
        $this->assertSame('name LIKE :name', $whereStr);
        $this->assertSame($bindings, $params);
    }

    public function testBuildWhereQueryWithNotLike(): void
    {
        $bindings = [':email' => '%@example.com'];
        [$whereStr, $params] = $this->_testBuildWhereQuery('email NOT LIKE :email', $bindings);
        $this->assertSame('email NOT LIKE :email', $whereStr);
        $this->assertSame($bindings, $params);
    }

    public function testBuildWhereQueryWithRegexp(): void
    {
        $bindings = [':pattern' => '^[a-z]+@[a-z]+\.[a-z]+$'];
        [$whereStr, $params] = $this->_testBuildWhereQuery('email REGEXP :pattern', $bindings);
        $this->assertSame('email REGEXP :pattern', $whereStr);
        $this->assertSame($bindings, $params);
    }

    public function testBuildWhereQueryWithNotRegexp(): void
    {
        $bindings = [':pattern' => '^[A-Z][a-z]+$'];
        [$whereStr, $params] = $this->_testBuildWhereQuery('name NOT REGEXP :pattern', $bindings);
        $this->assertSame('name NOT REGEXP :pattern', $whereStr);
        $this->assertSame($bindings, $params);
    }

    public function testBuildWhereQueryWithComplexConditions(): void
    {
        $bindings = [':name' => '%John%', ':status' => 'active'];
        [$whereStr, $params] = $this->_testBuildWhereQuery(
            'email IS NOT NULL AND name LIKE :name AND status = :status',
            $bindings
        );
        $this->assertSame('email IS NOT NULL AND name LIKE :name AND status = :status', $whereStr);
        $this->assertSame($bindings, $params);
    }

    public function testBuildWhereQueryWithEmptyString(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('');
        $this->assertSame('', $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithRawCondition(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('email IS NULL');
        $this->assertSame('email IS NULL', $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithRawConditionNoBindings(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('email IS NULL AND status = "active"');
        $this->assertSame('email IS NULL AND status = "active"', $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithRawConditionNoPlaceholders(): void
    {
        $w = 'email IS NULL AND status = "active" AND created_at < NOW()';
        [$whereStr, $params] = $this->_testBuildWhereQuery($w);
        $this->assertSame($w, $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithComplexRawCondition(): void
    {
        $w = 'email IS NULL AND status = "active" AND created_at < NOW() AND (role = "admin" OR role = "moderator")';
        [$whereStr, $params] = $this->_testBuildWhereQuery($w);
        $this->assertSame($w, $whereStr);
        $this->assertSame([], $params);
    }

    public function testBuildWhereQueryWithMixedConditions(): void
    {
        $bindings = [':name' => '%John%', ':status' => 'active', ':roles' => ['admin', 'moderator']];
        [$whereStr, $params] = $this->_testBuildWhereQuery(
            'email IS NOT NULL AND name LIKE :name AND status = :status AND role IN (:roles) AND created_at < NOW()',
            $bindings
        );
        $this->assertSame(
            'email IS NOT NULL AND name LIKE :name AND status = :status AND role IN (:roles0,:roles1) AND created_at < NOW()',
            $whereStr
        );
        $this->assertSame([
            ':name' => '%John%',
            ':status' => 'active',
            ':roles0' => 'admin',
            ':roles1' => 'moderator',
        ], $params);
    }

    public function testBuildWhereQueryWithMixedConditionsAndArrays(): void
    {
        $where = ['status' => 'active', 'role' => 'admin', 'created_at' => '2024-01-01'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);
        $this->assertSame(
            'status = :where_status AND role = :where_role AND created_at = :where_created_at',
            $whereStr
        );
        $this->assertSame([
            ':where_status' => 'active',
            ':where_role' => 'admin',
            ':where_created_at' => '2024-01-01',
        ], $params);
    }

    public function testBuildWhereQueryWithNamedPlaceholders(): void
    {
        $bindings = [':email' => 'john@example.com', ':name' => 'John'];
        [$whereStr, $params] = $this->_testBuildWhereQuery('email = :email AND name = :name', $bindings);
        $this->assertSame('email = :email AND name = :name', $whereStr);
        $this->assertSame($bindings, $params);
    }

    public function testBuildWhereQueryWithScalarBindingsAndPlaceholders(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('id = ?', 1);
        $this->assertSame('id = :where_0', $whereStr);
        $this->assertSame([':where_0' => 1], $params);
    }

    public function testBuildWhereQueryWithScalarBindingsAndNamedPlaceholders(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('id = :id', 1);
        $this->assertSame('id = :id', $whereStr);
        $this->assertSame([1], $params);
    }

    // ——— buildSqlClause & convertToNamedPlaceholders ———

    public function testBuildSqlClauseWithKeyValuePairs(): void
    {
        $data = ['name' => 'John', 'email' => 'john@example.com'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'set_');
        $this->assertSame('name = :set_name, email = :set_email', $sql);
        $this->assertSame([':set_name' => 'John', ':set_email' => 'john@example.com'], $params);
    }

    public function testBuildSqlClauseWithRawSql(): void
    {
        $data = ['name' => 'John', 'created_at = NOW()'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'set_');
        $this->assertStringContainsString('name = :set_name', $sql);
        $this->assertStringContainsString('created_at = NOW()', $sql);
        $this->assertSame([':set_name' => 'John'], $params);
    }

    public function testBuildSqlClauseWithQuestionMarkPlaceholder(): void
    {
        $data = ['name' => 'John', 'last_login > ?' => '2024-01-01'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'update_');
        $this->assertStringContainsString('name = :update_name', $sql);
        $this->assertStringContainsString('last_login > :update_0', $sql);
        $this->assertSame([':update_name' => 'John', ':update_0' => '2024-01-01'], $params);
    }

    public function testBuildSqlClauseWithCustomJoiner(): void
    {
        $data = ['name' => 'John', 'email' => 'john@example.com'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'where_', ' AND ');
        $this->assertSame('name = :where_name AND email = :where_email', $sql);
        $this->assertSame([':where_name' => 'John', ':where_email' => 'john@example.com'], $params);
    }

    public function testBuildSqlClauseWithQualifiedColumns(): void
    {
        $data = ['u.id' => 1, 'p.status' => 'active'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'where_', ' AND ');
        $this->assertSame('u.id = :where_u_dot_id AND p.status = :where_p_dot_status', $sql);
        $this->assertSame([':where_u_dot_id' => 1, ':where_p_dot_status' => 'active'], $params);
    }

    public function testConvertToNamedPlaceholders(): void
    {
        $query = 'SELECT * FROM users WHERE id = ? AND name = ?';
        $bindings = [1, 'John'];
        [$newQuery, $newBindings] = $this->_testConvertToNamedPlaceholders($query, $bindings, 'where_');
        $this->assertSame('SELECT * FROM users WHERE id = :where_0 AND name = :where_1', $newQuery);
        $this->assertSame([':where_0' => 1, ':where_1' => 'John'], $newBindings);
    }

    public function testConvertToNamedPlaceholdersWithNoPlaceholders(): void
    {
        $query = 'SELECT * FROM users WHERE id = :id';
        $bindings = [':id' => 1];
        [$newQuery, $newBindings] = $this->_testConvertToNamedPlaceholders($query, $bindings, 'where_');
        $this->assertSame($query, $newQuery);
        $this->assertSame([':id' => 1], $newBindings);
    }

    public function testConvertToNamedPlaceholdersWithMixedPlaceholders(): void
    {
        $query = 'SELECT * FROM users WHERE id = :id AND name = ? AND status = ?';
        $bindings = [':id' => 1, 'John', 'active'];
        [$newQuery, $newBindings] = $this->_testConvertToNamedPlaceholders($query, $bindings, 'where_');
        $this->assertSame(
            'SELECT * FROM users WHERE id = :id AND name = :where_0 AND status = :where_1',
            $newQuery
        );
        $this->assertSame([':id' => 1, ':where_0' => 'John', ':where_1' => 'active'], $newBindings);
    }
}
