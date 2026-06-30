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

    /**
     * `:named` and `?` in one WHERE string — positional values use integer keys in the bindings array.
     */
    public function testBuildWhereQueryWithMixedNamedAndQuestionMarkPlaceholders(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery(
            'status = :status AND id > ? AND role = :role',
            [':status' => 'active', 5, ':role' => 'editor']
        );
        $this->assertSame('status = :status AND id > :where_0 AND role = :role', $whereStr);
        $this->assertEquals([
            ':status' => 'active',
            ':where_0' => 5,
            ':role' => 'editor',
        ], $params);
    }

    /**
     * Mixed placeholders inside an associative WHERE array (raw SQL entry with both styles).
     */
    public function testBuildWhereQueryWithMixedPlaceholdersInAssociativeWhere(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery(
            ['status = :status AND id > ?'],
            [':status' => 'active', 5]
        );
        $this->assertSame('status = :status AND id > :where_raw_0', $whereStr);
        $this->assertSame([':status' => 'active', ':where_raw_0' => 5], $params);
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

    public function testBuildWhereQueryWithCustomPrefix(): void
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('id', 42, 'b0_w_');
        $this->assertSame('id = :b0_w_id', $whereStr);
        $this->assertSame([':b0_w_id' => 42], $params);

        [$whereStr, $params] = $this->_testBuildWhereQuery(['status' => 'active'], null, 'b1_w_');
        $this->assertSame('status = :b1_w_status', $whereStr);
        $this->assertSame([':b1_w_status' => 'active'], $params);

        [$whereStr, $params] = $this->_testBuildWhereQuery('email = ? AND id = ?', ['a@b.com', 5], 'b2_w_');
        $this->assertSame('email = :b2_w_0 AND id = :b2_w_1', $whereStr);
        $this->assertSame([':b2_w_0' => 'a@b.com', ':b2_w_1' => 5], $params);
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

    // ——— batch update SQL parts ———

    /**
     * Trait host with a public update() that records the last SQL handed to prepExec,
     * so tests can assert the exact statement shape while still executing it for real.
     */
    private function newUpdateSqlCapturer(): object
    {
        return new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                update as public;
                prepExec as traitPrepExec;
            }

            public string $lastSql = '';

            public function __construct(PDO $pdo)
            {
                $this->pdo = $pdo;
            }

            protected function prepExec(string $sql, array|string|int|null $params = null): PDOStatement
            {
                $this->lastSql = $sql;
                return $this->traitPrepExec($sql, $params);
            }
        };
    }

    public function testBuildUpdateSqlPartsSingleRowUsesCaseSyntax(): void
    {
        // buildUpdateSqlParts always emits CASE/WHEN — a batch of one is still a batch.
        // Plain single updates never reach it; update() routes them through buildSqlClause/buildWhereQuery.
        $method = new \ReflectionMethod(self::class, 'buildUpdateSqlParts');
        $method->setAccessible(true);
        [$set, $where, $params] = $method->invoke($this,
            [['name' => 'A', 'touched_at = NOW()']],
            [['id' => 1]],
            [null]
        );

        $this->assertSame(
            'name = CASE WHEN id = :b0_w_id THEN :b0_s0_name ELSE name END, '
            . 'touched_at = CASE WHEN id = :b0_w_id THEN NOW() ELSE touched_at END',
            $set
        );
        $this->assertSame('(id = :b0_w_id)', $where);
        $this->assertSame([':b0_w_id' => 1, ':b0_s0_name' => 'A'], $params);
    }

    public function testSingleUpdateBuildsClassicSql(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name, status) VALUES (20, 'Before', 'active')");
        $trait = $this->newUpdateSqlCapturer();

        $rows = $trait->update($t, ['name' => 'After'], 'id', 20);

        $this->assertSame(1, $rows);
        $this->assertSame("UPDATE {$t} SET name = :set_name WHERE id = :where_id", $trait->lastSql);
        $this->assertSame('After', $this->pdo->query("SELECT name FROM {$t} WHERE id = 20")->fetchColumn());
    }

    public function testSingleUpdateWithSqlTailLimitsAffectedRows(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name, status) VALUES (30, 'A', 'active'), (31, 'B', 'active')");
        $trait = $this->newUpdateSqlCapturer();

        $rows = $trait->update($t, ['status' => 'archived'], ['status' => 'active'], null, 'ORDER BY id LIMIT 1');

        $this->assertSame(1, $rows);
        $this->assertStringEndsWith('ORDER BY id LIMIT 1', $trait->lastSql);
        // ORDER BY id → the lowest id is archived first, the other row is untouched.
        $this->assertSame('archived', $this->pdo->query("SELECT status FROM {$t} WHERE id = 30")->fetchColumn());
        $this->assertSame('active', $this->pdo->query("SELECT status FROM {$t} WHERE id = 31")->fetchColumn());
    }

    public function testBatchOfOneRowUpdateUsesCaseSql(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name) VALUES (40, 'Before')");
        $trait = $this->newUpdateSqlCapturer();

        // Wrapping args in one-element lists opts into the batch (CASE/WHEN) path.
        $rows = $trait->update($t, [['name' => 'After']], [['id' => 40]]);

        $this->assertSame(1, $rows);
        $this->assertStringContainsString('CASE WHEN id = :b0_w_id THEN :b0_s0_name ELSE name END', $trait->lastSql);
        $this->assertSame('After', $this->pdo->query("SELECT name FROM {$t} WHERE id = 40")->fetchColumn());
    }

    public function testBatchUpdateWithSqlTailLimitCapsTotalRows(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name) VALUES (50, 'Old A'), (51, 'Old B')");
        $trait = $this->newUpdateSqlCapturer();

        // sqlTail applies to the whole batch statement: LIMIT 1 caps total changed rows across all entries.
        $rows = $trait->update($t, [
            ['name' => 'New A'],
            ['name' => 'New B'],
        ], [
            ['id' => 50],
            ['id' => 51],
        ], null, 'ORDER BY id LIMIT 1');

        $this->assertSame(1, $rows);
        $this->assertSame('New A', $this->pdo->query("SELECT name FROM {$t} WHERE id = 50")->fetchColumn());
        $this->assertSame('Old B', $this->pdo->query("SELECT name FROM {$t} WHERE id = 51")->fetchColumn());
    }

    public function testBuildBatchUpdateSqlParts(): void
    {
        $method = new \ReflectionMethod(self::class, 'buildUpdateSqlParts');
        $method->setAccessible(true);
        [$set, $where, $params] = $method->invoke($this,
            [
                ['name' => 'Alice', 'status' => 'active'],
                ['name' => 'Bob', 'status' => 'pending'],
            ],
            [
                ['id' => 1],
                ['id' => 2],
            ],
            null
        );

        $this->assertStringContainsString('name = CASE', $set);
        $this->assertStringContainsString('status = CASE', $set);
        $this->assertStringContainsString('ELSE name END', $set);
        $this->assertStringContainsString('WHEN id = :b0_w_id THEN :b0_s0_name', $set);
        $this->assertStringContainsString('WHEN id = :b1_w_id THEN :b1_s0_name', $set);
        $this->assertSame('(id = :b0_w_id) OR (id = :b1_w_id)', $where);
        $this->assertSame('Alice', $params[':b0_s0_name']);
        $this->assertSame('Bob', $params[':b1_s0_name']);
        $this->assertSame(1, $params[':b0_w_id']);
        $this->assertSame(2, $params[':b1_w_id']);
    }

    public function testIsBatchUpdatePayloadRequiresParallelLists(): void
    {
        $method = new \ReflectionMethod(self::class, 'isBatchUpdatePayload');
        $method->setAccessible(true);
        $this->assertFalse($method->invoke($this, [], []));
        $this->assertFalse($method->invoke($this, [['name' => 'only data']], 'id'));
        $this->assertFalse($method->invoke($this, [['name' => 'a']], ['id' => 1]));
        $this->assertFalse($method->invoke($this, [['name' => 'a']], [['id' => 1], ['id' => 2]]));
        $this->assertTrue($method->invoke($this, [['name' => 'A']], [['id' => 1]]));
        $this->assertTrue($method->invoke($this, [['name' => 'A'], ['name' => 'B']], [['id' => 1], ['id' => 2]]));
        $this->assertFalse($method->invoke($this, ['not-a-map'], [['id' => 1]]));
        $this->assertFalse($method->invoke($this, [['name' => 'a']], [123]));
    }

    public function testBatchUpdateWithColumnShorthandWhereList(): void
    {
        $method = new \ReflectionMethod(self::class, 'buildUpdateSqlParts');
        $method->setAccessible(true);
        [$set, $where, $params] = $method->invoke($this,
            [['name' => 'A'], ['name' => 'B']],
            ['id', 'id'],
            [10, 11]
        );
        $this->assertStringContainsString('WHEN id = :b0_w_id THEN :b0_s0_name', $set);
        $this->assertSame('(id = :b0_w_id) OR (id = :b1_w_id)', $where);
        $this->assertSame(10, $params[':b0_w_id']);
        $this->assertSame(11, $params[':b1_w_id']);
    }

    public function testBatchUpdateDocComplexMixedWhereBindings(): void
    {
        // Mirrors the 4-row multi update example in update() PHPDoc.
        $method = new \ReflectionMethod(self::class, 'buildUpdateSqlParts');
        $method->setAccessible(true);
        $since = '2024-01-01';
        $teamIds = [10, 11, 12];
        [$set, $where, $params] = $method->invoke($this,
            [
                ['name' => 'Alice', 'views = views + 1'],
                ['name' => 'Carol'],
                ['name' => 'Bob', 'views = views + ?' => 3],
                ['name' => 'Dave'],
            ],
            [
                ['id' => 1, 'email_verified != 0', 'created_at > :since', 'id IN (:ids)'],
                ['id' => 3],
                'id = ? AND role = ?',
                'id = 4 AND created_at > ?',
            ],
            [
                [':since' => $since, ':ids' => $teamIds],
                null,
                [99, 'editor'],
                $since,
            ]
        );

        // Row 0 SET: bound column + raw SQL expression (numeric-key list entry).
        $this->assertStringContainsString('WHEN id = :b0_w_id AND email_verified != 0 AND created_at > :since AND id IN (:ids0,:ids1,:ids2) THEN :b0_s0_name', $set);
        $this->assertStringContainsString('THEN views + 1', $set);
        $this->assertStringContainsString('ELSE views END', $set);
        // Row 1: associative WHERE only — no external bindings (`null` in the parallel list).
        $this->assertStringContainsString('WHEN id = :b1_w_id THEN :b1_s0_name', $set);
        // Row 2 SET: `?` key binds the increment; WHERE uses positional `?`.
        $this->assertStringContainsString('views + :b2_s1_0', $set);
        $this->assertStringContainsString('WHEN id = :b2_w_0 AND role = :b2_w_1', $set);
        // Row 3: string WHERE with a single `?` — `$bindings` entry is the scalar value.
        $this->assertStringContainsString('WHEN id = 4 AND created_at > :b3_w_0 THEN :b3_s0_name', $set);
        $this->assertSame($since, $params[':since']);
        $this->assertSame(3, $params[':b1_w_id']);
        $this->assertSame(99, $params[':b2_w_0']);
        $this->assertSame('editor', $params[':b2_w_1']);
        $this->assertSame($since, $params[':b3_w_0']);
    }

    public function testSingleUpdateWithRawSqlExpressionInSet(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("ALTER TABLE {$t} ADD COLUMN views INT NOT NULL DEFAULT 0");
        $this->pdo->exec("INSERT INTO {$t} (id, name, status, views) VALUES (60, 'Before', 'active', 5)");
        $trait = $this->newUpdateSqlCapturer();

        $rows = $trait->update($t, ['name' => 'After', 'views = views + 1'], 'id', 60);

        $this->assertSame(1, $rows);
        $this->assertSame("UPDATE {$t} SET name = :set_name, views = views + 1 WHERE id = :where_id", $trait->lastSql);
        $this->assertSame(6, (int) $this->pdo->query("SELECT views FROM {$t} WHERE id = 60")->fetchColumn());
    }

    /**
     * Single update WHERE with both `:named` and `?` placeholders in one clause.
     */
    public function testSingleUpdateWithMixedNamedAndQuestionMarkPlaceholders(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name, status) VALUES (61, 'Mixed', 'active')");
        $trait = $this->newUpdateSqlCapturer();

        $rows = $trait->update(
            $t,
            ['name' => 'Updated'],
            'status = :status AND id > ?',
            [':status' => 'active', 60]
        );

        $this->assertSame(1, $rows);
        $this->assertSame(
            "UPDATE {$t} SET name = :set_name WHERE status = :status AND id > :where_0",
            $trait->lastSql
        );
        $this->assertSame('Updated', $this->pdo->query("SELECT name FROM {$t} WHERE id = 61")->fetchColumn());
    }

    public function testBatchUpdateSupportsFullSingleRowFeatureSetPerEntry(): void
    {
        $method = new \ReflectionMethod(self::class, 'buildUpdateSqlParts');
        $method->setAccessible(true);
        [$set, $where, $params] = $method->invoke($this,
            [
                ['views = views + ?' => 5, 'touched_at = NOW()'],
                ['views' => 0],
            ],
            ['id = ? AND status = ?', ['id' => 2]],
            [[7, 'active'], null]
        );

        // Row 0: '?'-key SET entry and raw SQL entry, string WHERE with per-row bindings.
        $this->assertStringContainsString('views = CASE WHEN id = :b0_w_0 AND status = :b0_w_1 THEN views + :b0_s0_0', $set);
        $this->assertStringContainsString('touched_at = CASE WHEN id = :b0_w_0 AND status = :b0_w_1 THEN NOW() ELSE touched_at END', $set);
        $this->assertStringContainsString('WHEN id = :b1_w_id THEN :b1_s0_views ELSE views END', $set);
        $this->assertSame('(id = :b0_w_0 AND status = :b0_w_1) OR (id = :b1_w_id)', $where);
        $this->assertSame(
            [':b0_w_0' => 7, ':b0_w_1' => 'active', ':b0_s0_0' => 5, ':b1_w_id' => 2, ':b1_s0_views' => 0],
            $params
        );
    }

    public function testBatchUpdateRunsAgainstMysql(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name, status) VALUES (10, 'Old A', 'active'), (11, 'Old B', 'active')");
        $trait = $this->newUpdateSqlCapturer();

        $rows = $trait->update($t, [
            ['name' => 'New A'],
            ['name' => 'New B'],
        ], [
            ['id' => 10],
            ['id' => 11],
        ]);

        $this->assertSame(2, $rows);
        $this->assertStringContainsString('CASE WHEN', $trait->lastSql);
        $this->assertSame('New A', $this->pdo->query("SELECT name FROM {$t} WHERE id = 10")->fetchColumn());
        $this->assertSame('New B', $this->pdo->query("SELECT name FROM {$t} WHERE id = 11")->fetchColumn());
    }

    /**
     * Rows whose WHERE needs no external bindings may use `null`, `[]`, or simply omit
     * their trailing `$bindings` entry — only rows with placeholders need one.
     */
    public function testBatchUpdatePerRowBindingsMayBeNullEmptyOrOmitted(): void
    {
        $t = self::TABLE_USERS;
        $this->pdo->exec("INSERT INTO {$t} (id, name, status) VALUES (70, 'A', 'active'), (71, 'B', 'active'), (72, 'C', 'active')");
        $trait = $this->newUpdateSqlCapturer();

        $rows = $trait->update($t, [
            ['name' => 'A2'],
            ['name' => 'B2'],
            ['name' => 'C2'],
        ], [
            'id = ?',      // needs a binding
            ['id' => 71],  // key-value WHERE: binds itself, entry stays null
            ['id' => 72],  // entry omitted entirely (bindings list is shorter than rows)
        ], [
            [70],
            null,
        ]);

        $this->assertSame(3, $rows);
        $this->assertSame('A2', $this->pdo->query("SELECT name FROM {$t} WHERE id = 70")->fetchColumn());
        $this->assertSame('B2', $this->pdo->query("SELECT name FROM {$t} WHERE id = 71")->fetchColumn());
        $this->assertSame('C2', $this->pdo->query("SELECT name FROM {$t} WHERE id = 72")->fetchColumn());
    }

    // ——— doc examples (helper PHPDoc blocks) ———

    /** buildInQuery() doc example */
    public function testDocBuildInQueryExample(): void
    {
        [$sql, $params] = $this->_testBuildInQuery(
            'SELECT * FROM users WHERE id IN (:ids)',
            ['ids' => [1, 2, 3]]
        );
        $this->assertSame('SELECT * FROM users WHERE id IN (:ids0,:ids1,:ids2)', $sql);
        $this->assertSame([':ids0' => 1, ':ids1' => 2, ':ids2' => 3], $params);
    }

    /** buildSqlClause() doc — comma joiner with insert_ prefix */
    public function testDocBuildSqlClauseInsertCommaExample(): void
    {
        [$sql, $params] = $this->_testBuildSqlClause(
            ['name' => 'John', 'email' => 'john@example.com', 'created_at = NOW()'],
            'insert_'
        );
        $this->assertSame('name = :insert_name, email = :insert_email, created_at = NOW()', $sql);
        $this->assertSame([':insert_name' => 'John', ':insert_email' => 'john@example.com'], $params);
    }

    /** buildSqlClause() doc — AND joiner with insert_ prefix */
    public function testDocBuildSqlClauseInsertAndExample(): void
    {
        [$sql, $params] = $this->_testBuildSqlClause(
            ['name' => 'John', 'email' => 'john@example.com', 'created_at = NOW()'],
            'insert_',
            ' AND '
        );
        $this->assertSame(
            'name = :insert_name AND email = :insert_email AND created_at = NOW()',
            $sql
        );
        $this->assertSame([':insert_name' => 'John', ':insert_email' => 'john@example.com'], $params);
    }

    /** buildSqlClause() doc — `?` key with update_ prefix */
    public function testDocBuildSqlClauseUpdateQuestionMarkKeyExample(): void
    {
        [$sql, $params] = $this->_testBuildSqlClause(
            ['name' => 'John', 'email' => 'john@example,com', 'last_login = ?' => '2024-01-01'],
            'update_',
            ', '
        );
        $this->assertSame(
            'name = :update_name, email = :update_email, last_login = :update_0',
            $sql
        );
        $this->assertSame([
            ':update_name' => 'John',
            ':update_email' => 'john@example,com',
            ':update_0' => '2024-01-01',
        ], $params);
    }

    /** buildWhereQuery() doc example 2.b */
    public function testDocBuildWhereQuery2bRawSqlFragmentsExample(): void
    {
        $date = '2024-06-01';
        [$where, $params] = $this->_testBuildWhereQuery(
            ['id' => 1, 'status' => 'active', 'email_verified != 0', 'created_at > :date'],
            [':date' => $date]
        );
        $this->assertSame(
            'id = :where_id AND status = :where_status AND email_verified != 0 AND created_at > :date',
            $where
        );
        $this->assertEquals([
            ':where_id' => 1,
            ':where_status' => 'active',
            ':date' => $date,
        ], $params);
    }

    /** buildWhereQuery() doc example 3 */
    public function testDocBuildWhereQuery3EmailStatusExample(): void
    {
        [$where, $params] = $this->_testBuildWhereQuery(
            'email = ? AND status != ?',
            ['john@example.com', 'inactive']
        );
        $this->assertSame('email = :where_0 AND status != :where_1', $where);
        $this->assertSame([':where_0' => 'john@example.com', ':where_1' => 'inactive'], $params);
    }

    /** buildWhereQuery() doc example 7 */
    public function testDocBuildWhereQuery7DirectBindingsInArrayExample(): void
    {
        $ids = [1, 2, 3];
        $date = '2024-01-01';
        [$where, $params] = $this->_testBuildWhereQuery([
            'id IN (?)' => $ids,
            'created_at > ?' => $date,
        ]);
        $this->assertStringContainsString('id IN (:where_00,:where_01,:where_02)', $where);
        $this->assertStringContainsString('created_at > :where_1', $where);
        $this->assertSame(1, $params[':where_00']);
        $this->assertSame($date, $params[':where_1']);
    }

    /** convertToNamedPlaceholders() doc — default nph_ prefix */
    public function testDocConvertToNamedPlaceholdersDefaultPrefixExample(): void
    {
        $query = 'SELECT * FROM users WHERE id = ? AND status = ?';
        $bindings = [5, 'active'];
        [$newQuery, $newBindings] = $this->_testConvertToNamedPlaceholders($query, $bindings);
        $this->assertSame('SELECT * FROM users WHERE id = :nph_0 AND status = :nph_1', $newQuery);
        $this->assertSame([':nph_0' => 5, ':nph_1' => 'active'], $newBindings);
    }

    /** buildInsertBatches() doc example */
    public function testDocBuildInsertBatchesExample(): void
    {
        $method = new \ReflectionMethod(self::class, 'buildInsertBatches');
        $method->setAccessible(true);
        $batches = $method->invoke($this, [
            ['name' => 'Ion', 'created_at = NOW()'],
            ['name' => 'Ani', 'created_at = NOW()'],
        ]);

        $this->assertArrayHasKey('name|created_at', $batches);
        [$columns, $valueRows, $params] = $batches['name|created_at'];
        $this->assertSame('name, created_at', $columns);
        $this->assertSame(['(:row_0_name, NOW())', '(:row_1_name, NOW())'], $valueRows);
        $this->assertSame([':row_0_name' => 'Ion', ':row_1_name' => 'Ani'], $params);
    }

    /** buildUpdateSqlParts() doc example — A/B rows */
    public function testDocBuildUpdateSqlPartsAbExample(): void
    {
        $method = new \ReflectionMethod(self::class, 'buildUpdateSqlParts');
        $method->setAccessible(true);
        [$set, $where, $params] = $method->invoke(
            $this,
            [['name' => 'A'], ['name' => 'B']],
            [['id' => 1], ['id' => 2]],
            null
        );

        $this->assertSame(
            'name = CASE WHEN id = :b0_w_id THEN :b0_s0_name WHEN id = :b1_w_id THEN :b1_s0_name ELSE name END',
            $set
        );
        $this->assertSame('(id = :b0_w_id) OR (id = :b1_w_id)', $where);
        $this->assertSame([':b0_w_id' => 1, ':b0_s0_name' => 'A', ':b1_w_id' => 2, ':b1_s0_name' => 'B'], $params);
    }
}
