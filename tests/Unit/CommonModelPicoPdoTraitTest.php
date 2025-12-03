<?php

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PHPUnit\Framework\TestCase;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\MockObject\MockObject;
use Error;
use Mockery\Adapter\Phpunit\MockeryTestCase;
use phpmock\MockBuilder;
use phpmock\phpunit\PHPMock;
use phpmock\Mock;

/**
 * Test class that uses the trait to access protected methods
 */
class CommonModelPicoPdoTraitTest extends MockeryTestCase
{
    use CommonModelPicoPdoTrait {
        prepExec as public _testPrepExec;
        buildWhereQuery as public _testBuildWhereQuery;
        exists as public _testExists;
        insert as public _testInsert;
        update as public _testUpdate;
        select as public _testSelect;
        selectOne as public _testSelectOne;
        selectJoin as public _testSelectJoin;
        delete as public _testDelete;
        buildInQuery as public _testBuildInQuery;
        buildSqlClause as public _testBuildSqlClause;
        convertToNamedPlaceholders as public _testConvertToNamedPlaceholders;
        getPdoDebug as public _testGetPdoDebug;
    }

    use PHPMock;

    protected PDO $pdo;
    protected PDOStatement $pdoStatement;
    protected $trait;
    private Mock $error_log_mock;
    private Mock $ob_start_mock;
    private Mock $ob_get_clean_mock;

    protected function setUp(): void
    {
        parent::setUp();
        
        // Redirect error logs to a temporary file
        ini_set('error_log', '/tmp/picopdo_test_errors.log');
        if (file_exists('/tmp/picopdo_test_errors.log')) {
            unlink('/tmp/picopdo_test_errors.log');
        }
        
        // Mock PDO and PDOStatement
        $this->pdo = $this->createMock(PDO::class);
        $this->pdoStatement = $this->createMock(PDOStatement::class);
        
        // Set up a custom error handler to silence error output
        set_error_handler(function($errno, $errstr, $errfile, $errline) {
            // Silently ignore errors during tests
            return true;
        });
            
        // Initialize the trait with a test class that makes methods public
        $this->trait = new class {
            use CommonModelPicoPdoTrait {
                prepExec as public;
                exists as public;
                buildWhereQuery as public;
                buildInQuery as public;
                select as public;
                selectOne as public;
                selectJoin as public;
                buildSqlClause as public;
                convertToNamedPlaceholders as public;
                getPdoDebug as public;
            }
            
            public function setPdo(PDO $pdo): void {
                $this->pdo = $pdo;
            }
        };
        $this->trait->setPdo($this->pdo);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        
        // Restore the default error handler
        restore_error_handler();
        
        // Clean up error log file
        if (file_exists('/tmp/picopdo_test_errors.log')) {
            unlink('/tmp/picopdo_test_errors.log');
        }
    }

    /**
     * Test prepExec with simple positional placeholder (from PHPDoc example)
     */
    public function testPrepExecWithSimplePositionalPlaceholder()
    {
        $sql = "SELECT * FROM users WHERE id = ?";
        $params = [5];
        
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT * FROM users WHERE id = :nph_0")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':nph_0' => 5])
            ->willReturn(true);
            
        $result = $this->_testPrepExec($sql, $params);
        
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    /**
     * Test prepExec with named placeholders (from PHPDoc example)
     */
    public function testPrepExecWithNamedPlaceholders()
    {
        $sql = "SELECT * FROM users WHERE id = :id";
        $params = ['id' => 10];
        
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($params)
            ->willReturn(true);
            
        $result = $this->_testPrepExec($sql, $params);
        
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    /**
     * Test prepExec with array parameters for IN clause (from PHPDoc example)
     */
    public function testPrepExecWithArrayParameters()
    {
        $sql = "SELECT * FROM users WHERE id IN (:ids)";
        $params = [':ids' => [1, 2, 3]];
        
        // First build the IN query
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        
        // Then test that prepExec forwards the expanded query
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($newSql)
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($newParams)
            ->willReturn(true);
            
        $result = $this->_testPrepExec($newSql, $newParams);
        
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    public function testPrepExecWithEmptySql()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('')
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $result = $this->_testPrepExec('');
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    public function testPrepExecWithFailedExecution()
    {
        $sql = "SELECT * FROM users WHERE id = :id";
        $params = ['id' => 1];
        
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($params)
            ->willThrowException(new PDOException('Failed to execute query: Test error'));
            
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('Failed to execute query: Test error');
        
        $this->_testPrepExec($sql, $params);
    }

    public function testPrepExecWithFailedPrepare()
    {
        $sql = "SELECT * FROM users WHERE id = :id";
        
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->willReturn(false);
            
        $this->expectException(Error::class);
        $this->expectExceptionMessageMatches('/Call to a member function execute\(\) on (?:bool|false)/');
        
        $this->_testPrepExec($sql);
    }

    /**
     * Test exists with classic key-value (from PHPDoc example)
     */
    public function testExistsWithKeyValue()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM users WHERE id = :where_id LIMIT 1")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_id' => 1])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchColumn')
            ->willReturn(1);
            
        $result = $this->_testExists('users', 'id', 1);
        $this->assertTrue($result);
    }

    /**
     * Test exists with associative array (from PHPDoc example)
     */
    public function testExistsWithAssociativeArray()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM users WHERE `status` = :where_status AND `email_verified` = :where_email_verified LIMIT 1")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_status' => 'active', ':where_email_verified' => 1])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchColumn')
            ->willReturn(1);
            
        $result = $this->_testExists('users', ['status' => 'active', 'email_verified' => 1]);
        $this->assertTrue($result);
    }

    /**
     * Test exists with custom WHERE clause (from PHPDoc example)
     */
    public function testExistsWithCustomWhereClause()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM users WHERE email = :where_0 AND created_at > :where_1 LIMIT 1")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 'user@example.com', ':where_1' => '2024-01-01'])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchColumn')
            ->willReturn(1);
            
        $result = $this->_testExists('users', 'email = ? AND created_at > ?', ['user@example.com', '2024-01-01']);
        $this->assertTrue($result);
    }

    /**
     * Test exists with null where
     */
    public function testExistsWithNullWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM users LIMIT 1")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchColumn')
            ->willReturn(1);
            
        $result = $this->_testExists('users', null);
        $this->assertTrue($result);
    }

    /**
     * Test exists with empty array where
     */
    public function testExistsWithEmptyArrayWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM users LIMIT 1")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchColumn')
            ->willReturn(1);
            
        $result = $this->_testExists('users', []);
        $this->assertTrue($result);
    }

    /**
     * Test buildInQuery with non-array values
     */
    public function testBuildInQueryWithNonArrayValues()
    {
        $sql = "SELECT * FROM users WHERE id = :id AND status = :status";
        $params = [':id' => 1, ':status' => 'active'];
        
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        
        // Should return unchanged SQL and params since there are no arrays
        $this->assertEquals($sql, $newSql);
        $this->assertEquals($params, $newParams);
    }

    public function testBuildWhereQueryWithArray()
    {
        $where = ['id' => 1, 'name' => 'John'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('`id` = :where_id AND `name` = :where_name', $whereStr);
        $this->assertEquals([
            ':where_id' => 1,
            ':where_name' => 'John'
        ], $params);
    }

    public function testBuildWhereQueryWithString()
    {
        $where = 'id';
        $bindings = 1;
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('id = :where_id', $whereStr);
        $this->assertEquals([':where_id' => 1], $params);
    }

    public function testBuildWhereQueryWithCustomCondition()
    {
        $where = 'status = :status AND created_at > :date';
        $bindings = [
            ':status' => 'active',
            ':date' => '2024-01-01'
        ];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('status = :status AND created_at > :date', $whereStr);
        $this->assertEquals($bindings, $params);
    }

    public function testBuildWhereQueryWithNullWhere()
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery(null);
        $this->assertEquals('', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithEmptyArray()
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery([]);
        $this->assertEquals('', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildInQuery()
    {
        $sql = "SELECT * FROM users WHERE id IN (:ids)";
        $params = [':ids' => [1, 2, 3]];
        
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        
        $this->assertEquals("SELECT * FROM users WHERE id IN (:ids0,:ids1,:ids2)", $newSql);
        $this->assertEquals([':ids0' => 1, ':ids1' => 2, ':ids2' => 3], $newParams);
    }

    public function testBuildInQueryWithEmptyInputs()
    {
        $sql = "";
        $params = [];
        
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        
        $this->assertEquals("", $newSql);
        $this->assertEquals([], $newParams);
    }

    public function testBuildInQueryWithNumericKeys()
    {
        $sql = "SELECT * FROM users WHERE id IN (?)";
        $params = [0 => [1, 2, 3]];
        
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        
        // Should not modify SQL or params since ? placeholders are not expanded by buildInQuery
        $this->assertEquals($sql, $newSql);
        $this->assertEquals([], $newParams);
    }

    public function testBuildInQueryWithMultipleArrays()
    {
        $sql = "SELECT * FROM users WHERE id IN (:ids) AND status IN (:statuses)";
        $params = [
            ':ids' => [1, 2, 3],
            ':statuses' => ['active', 'pending']
        ];
        
        $expectedSql = "SELECT * FROM users WHERE id IN (:ids0,:ids1,:ids2) AND status IN (:statuses0,:statuses1)";
        $expectedParams = [
            ':ids0' => 1,
            ':ids1' => 2,
            ':ids2' => 3,
            ':statuses0' => 'active',
            ':statuses1' => 'pending'
        ];
        
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($expectedParams)
            ->willReturn(true);
            
        $result = $this->_testPrepExec($sql, $params);
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    public function testBuildWhereQueryWithIsNull()
    {
        $where = 'email IS NULL';
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('email IS NULL', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithIsNotNull()
    {
        $where = 'email IS NOT NULL';
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('email IS NOT NULL', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithLike()
    {
        $where = 'name LIKE :name';
        $bindings = [':name' => '%John%'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('name LIKE :name', $whereStr);
        $this->assertEquals($bindings, $params);
    }

    public function testBuildWhereQueryWithNotLike()
    {
        $where = 'email NOT LIKE :email';
        $bindings = [':email' => '%@example.com'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('email NOT LIKE :email', $whereStr);
        $this->assertEquals($bindings, $params);
    }

    public function testBuildWhereQueryWithRegexp()
    {
        $where = 'email REGEXP :pattern';
        $bindings = [':pattern' => '^[a-z]+@[a-z]+\.[a-z]+$'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('email REGEXP :pattern', $whereStr);
        $this->assertEquals($bindings, $params);
    }

    public function testBuildWhereQueryWithNotRegexp()
    {
        $where = 'name NOT REGEXP :pattern';
        $bindings = [':pattern' => '^[A-Z][a-z]+$'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('name NOT REGEXP :pattern', $whereStr);
        $this->assertEquals($bindings, $params);
    }

    public function testBuildWhereQueryWithComplexConditions()
    {
        $where = 'email IS NOT NULL AND name LIKE :name AND status = :status';
        $bindings = [
            ':name' => '%John%',
            ':status' => 'active'
        ];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('email IS NOT NULL AND name LIKE :name AND status = :status', $whereStr);
        $this->assertEquals($bindings, $params);
    }

    public function testBuildWhereQueryWithEmptyString()
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('');
        $this->assertEquals('', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithEmptyStringMandatory()
    {
        [$whereStr, $params] = $this->_testBuildWhereQuery('');
        $this->assertEquals('', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithRawCondition()
    {
        $where = 'email IS NULL';
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('email IS NULL', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithRawConditionNoBindings()
    {
        $where = 'email IS NULL AND status = "active"';
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('email IS NULL AND status = "active"', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithRawConditionNoPlaceholders()
    {
        $where = 'email IS NULL AND status = "active" AND created_at < NOW()';
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('email IS NULL AND status = "active" AND created_at < NOW()', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithComplexRawCondition()
    {
        $where = 'email IS NULL AND status = "active" AND created_at < NOW() AND (role = "admin" OR role = "moderator")';
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('email IS NULL AND status = "active" AND created_at < NOW() AND (role = "admin" OR role = "moderator")', $whereStr);
        $this->assertEmpty($params);
    }

    public function testBuildWhereQueryWithMixedConditions()
    {
        $where = 'email IS NOT NULL AND name LIKE :name AND status = :status AND role IN (:roles) AND created_at < NOW()';
        $bindings = [
            ':name' => '%John%',
            ':status' => 'active',
            ':roles' => ['admin', 'moderator']
        ];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('email IS NOT NULL AND name LIKE :name AND status = :status AND role IN (:roles0,:roles1) AND created_at < NOW()', $whereStr);
        $this->assertEquals([
            ':name' => '%John%',
            ':status' => 'active',
            ':roles0' => 'admin',
            ':roles1' => 'moderator'
        ], $params);
    }

    public function testBuildWhereQueryWithMixedConditionsAndArrays()
    {
        $where = ['status' => 'active', 'role' => 'admin', 'created_at' => '2024-01-01'];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where);

        $this->assertEquals('`status` = :where_status AND `role` = :where_role AND `created_at` = :where_created_at', $whereStr);
        $this->assertEquals([
            ':where_status' => 'active',
            ':where_role' => 'admin',
            ':where_created_at' => '2024-01-01'
        ], $params);
    }

    public function testBuildWhereQueryWithNamedPlaceholders()
    {
        $where = 'email = :email AND name = :name';
        $bindings = [
            ':email' => 'john@example.com',
            ':name' => 'John'
        ];
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('email = :email AND name = :name', $whereStr);
        $this->assertEquals($bindings, $params);
    }

    /**
     * Test that IN queries work correctly with named parameters
     */
    public function testInQueryWithNamedParameters()
    {
        $sql = "SELECT * FROM users WHERE status IN (:statuses)";
        $params = [':statuses' => ['active', 'pending']];
        
        $expectedSql = "SELECT * FROM users WHERE status IN (:statuses0,:statuses1)";
        $expectedParams = [':statuses0' => 'active', ':statuses1' => 'pending'];
        
        // Test that the SQL query is prepared with expanded IN clause
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->pdoStatement);
            
        // Test that the parameters are executed with expanded values
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($expectedParams)
            ->willReturn(true);
            
        $result = $this->_testPrepExec($sql, $params);
        
        // Test that the result is a PDOStatement
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    /**
     * Test that multiple IN queries work correctly with named parameters
     */
    public function testMultipleInQueriesWithNamedParameters()
    {
        $sql = "SELECT * FROM users WHERE status IN (:statuses) AND role IN (:roles)";
        $params = [
            ':statuses' => ['active', 'pending'],
            ':roles' => ['admin', 'user']
        ];
        
        $expectedSql = "SELECT * FROM users WHERE status IN (:statuses0,:statuses1) AND role IN (:roles0,:roles1)";
        $expectedParams = [
            ':statuses0' => 'active',
            ':statuses1' => 'pending',
            ':roles0' => 'admin',
            ':roles1' => 'user'
        ];
        
        // Test that the SQL query is prepared with expanded IN clauses
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->pdoStatement);
            
        // Test that the parameters are executed with expanded values
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($expectedParams)
            ->willReturn(true);
            
        $result = $this->_testPrepExec($sql, $params);
        
        // Test that the result is a PDOStatement
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    public function testBuildInQueryWithArrayParameters()
    {
        $sql = "SELECT * FROM users WHERE id IN (:ids)";
        $params = [':ids' => [1, 2, 3]];
        
        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);
        
        $this->assertEquals("SELECT * FROM users WHERE id IN (:ids0,:ids1,:ids2)", $newSql);
        $this->assertEquals([':ids0' => 1, ':ids1' => 2, ':ids2' => 3], $newParams);
    }

    public function testSelectWithNullWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM users")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe', 'email' => 'john@example.com']);
            
        $stmt = $this->_testSelect('users', ['name', 'email'], null);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(['name' => 'John Doe', 'email' => 'john@example.com'], $result);
    }

    public function testSelectWithEmptyArrayWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM users")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe', 'email' => 'john@example.com']);
            
        $stmt = $this->_testSelect('users', ['name', 'email'], []);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(['name' => 'John Doe', 'email' => 'john@example.com'], $result);
    }

    public function testSelectAllWithNullWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM users")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['name' => 'John Doe', 'email' => 'john@example.com'],
                ['name' => 'Jane Smith', 'email' => 'jane@example.com']
            ]);
            
        $stmt = $this->_testSelect('users', ['name', 'email'], null);
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals([
            ['name' => 'John Doe', 'email' => 'john@example.com'],
            ['name' => 'Jane Smith', 'email' => 'jane@example.com']
        ], $result);
    }

    public function testSelectAllWithEmptyArrayWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM users")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['name' => 'John Doe', 'email' => 'john@example.com'],
                ['name' => 'Jane Smith', 'email' => 'jane@example.com']
            ]);
            
        $stmt = $this->_testSelect('users', ['name', 'email'], []);
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals([
            ['name' => 'John Doe', 'email' => 'john@example.com'],
            ['name' => 'Jane Smith', 'email' => 'jane@example.com']
        ], $result);
    }

    public function testSelectWithPlaceholdersInColumnsAndNullWhere(): void
    {
        $columns = [
            "CONCAT(first_name, ' ', last_name) AS full_name",
            "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
        ];
        $bindings = ['report_date' => '2024-01-01'];

        $expectedSql = "SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users";
        $expectedParams = $bindings;

        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($expectedParams)
            ->willReturn(true);

        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['full_name' => 'John Doe', 'age' => 30]);

        $stmt = $this->_testSelect('users', $columns, null, $bindings);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertEquals(['full_name' => 'John Doe', 'age' => 30], $result);
    }

    public function testSelectWithPlaceholdersInColumnsAndEmptyWhere(): void
    {
        $columns = [
            "CONCAT(first_name, ' ', last_name) AS full_name",
            "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
        ];
        $bindings = ['report_date' => '2024-01-01'];

        $expectedSql = "SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users";
        $expectedParams = $bindings;

        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($expectedParams)
            ->willReturn(true);

        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['full_name' => 'John Doe', 'age' => 30]);

        $stmt = $this->_testSelect('users', $columns, '', $bindings);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertEquals(['full_name' => 'John Doe', 'age' => 30], $result);
    }

    public function testSelectWithPlaceholdersInColumnsAndEmptyArrayWhere(): void
    {
        $columns = [
            "CONCAT(first_name, ' ', last_name) AS full_name",
            "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
        ];
        $bindings = ['report_date' => '2024-01-01'];

        $expectedSql = "SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users";
        $expectedParams = $bindings;

        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($expectedParams)
            ->willReturn(true);

        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['full_name' => 'John Doe', 'age' => 30]);

        $stmt = $this->_testSelect('users', $columns, [], $bindings);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertEquals(['full_name' => 'John Doe', 'age' => 30], $result);
    }

    public function testSelectWithPlaceholdersInColumnsAndWhere(): void
    {
        $columns = [
            "CONCAT(first_name, ' ', last_name) AS full_name",
            "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
        ];
        $where = ['status' => 'active'];
        $bindings = ['report_date' => '2024-01-01'];

        $expectedSql = "SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users WHERE `status` = :where_status";
        $expectedParams = array_merge($bindings, [':where_status' => 'active']);

        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with($expectedParams)
            ->willReturn(true);

        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['full_name' => 'John Doe', 'age' => 30]);

        $stmt = $this->_testSelect('users', $columns, $where, $bindings);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertEquals(['full_name' => 'John Doe', 'age' => 30], $result);
    }

    public function testSelectAllWithPlaceholdersInColumnsAndNullWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':report_date' => '2024-01-01'])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['full_name' => 'John Doe', 'age' => 30],
                ['full_name' => 'Jane Smith', 'age' => 25]
            ]);
            
        $stmt = $this->_testSelect('users', 
            [
                "CONCAT(first_name, ' ', last_name) AS full_name",
                "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
            ],
            null,
            [':report_date' => '2024-01-01']
        );
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        
        $this->assertEquals([
            ['full_name' => 'John Doe', 'age' => 30],
            ['full_name' => 'Jane Smith', 'age' => 25]
        ], $result);
    }

    public function testSelectAllWithPlaceholdersInColumnsAndEmptyWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':report_date' => '2024-01-01'])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['full_name' => 'John Doe', 'age' => 30],
                ['full_name' => 'Jane Smith', 'age' => 25]
            ]);
            
        $stmt = $this->_testSelect('users', 
            [
                "CONCAT(first_name, ' ', last_name) AS full_name",
                "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
            ],
            '',
            [':report_date' => '2024-01-01']
        );
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        
        $this->assertEquals([
            ['full_name' => 'John Doe', 'age' => 30],
            ['full_name' => 'Jane Smith', 'age' => 25]
        ], $result);
    }

    public function testSelectAllWithPlaceholdersInColumnsAndEmptyArrayWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':report_date' => '2024-01-01'])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['full_name' => 'John Doe', 'age' => 30],
                ['full_name' => 'Jane Smith', 'age' => 25]
            ]);
            
        $stmt = $this->_testSelect('users', 
            [
                "CONCAT(first_name, ' ', last_name) AS full_name",
                "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
            ],
            [],
            [':report_date' => '2024-01-01']
        );
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        
        $this->assertEquals([
            ['full_name' => 'John Doe', 'age' => 30],
            ['full_name' => 'Jane Smith', 'age' => 25]
        ], $result);
    }

    public function testSelectAllWithPlaceholdersInColumnsAndWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT CONCAT(first_name, ' ', last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM users WHERE status = :where_0")
            ->willReturn($this->pdoStatement);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([
                ':report_date' => '2024-01-01',
                ':where_0' => 'active'
            ])
            ->willReturn(true);
            
        $this->pdoStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['full_name' => 'John Doe', 'age' => 30],
                ['full_name' => 'Jane Smith', 'age' => 25]
            ]);
            
        $stmt = $this->_testSelect('users', 
            [
                "CONCAT(first_name, ' ', last_name) AS full_name",
                "TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age"
            ],
            'status = ?',
            [':report_date' => '2024-01-01', 'active']
        );
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
        
        $this->assertEquals([
            ['full_name' => 'John Doe', 'age' => 30],
            ['full_name' => 'Jane Smith', 'age' => 25]
        ], $result);
    }

    public function testGetPdoDebugWithFalseStatement()
    {
        $result = $this->getPdoDebug(false);
        $this->assertEquals('Statement preparation failed', $result);
    }

    public function testBuildWhereQueryWithScalarBindingsAndPlaceholders()
    {
        $where = 'id = ?';
        $bindings = 1;
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('id = :where_0', $whereStr);
        $this->assertEquals([':where_0' => 1], $params);
    }

    public function testBuildWhereQueryWithScalarBindingsAndNamedPlaceholders()
    {
        $where = 'id = :id';
        $bindings = 1;
        [$whereStr, $params] = $this->_testBuildWhereQuery($where, $bindings);

        $this->assertEquals('id = :id', $whereStr);
        $this->assertEquals([1], $params);
    }

    public function testPrepExecWithLodurTestServerErrorLogging()
    {
        // Define LODUR_TEST_SERVER constant to trigger error logging path
        if (!defined('LODUR_TEST_SERVER')) {
            define('LODUR_TEST_SERVER', true);
        }

        // Create mock for error_log function
        $errorLogMock = $this->getFunctionMock('Lodur\PicoPdo', 'error_log');
        $errorLogMock->expects($this->atLeastOnce())
            ->willReturnCallback(function($message) {
                // Verify error log is called with expected format
                $this->assertStringContainsString('<br><b>', $message);
            });

        // Mock PDO to throw exception
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->willThrowException(new PDOException('Test error'));

        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('Test error');

        $this->_testPrepExec('SELECT * FROM users WHERE id = ?', [1]);
    }

    public function testBuildInQueryWithLodurTestServerErrorLogging()
    {
        // Define LODUR_TEST_SERVER constant to trigger error logging path
        if (!defined('LODUR_TEST_SERVER')) {
            define('LODUR_TEST_SERVER', true);
        }

        // Create mock for error_log function
        $errorLogMock = $this->getFunctionMock('Lodur\PicoPdo', 'error_log');
        $errorLogMock->expects($this->once())
            ->with($this->stringContains('Provided array for IN clause is empty or key is numeric'));

        // Test with numeric key (should trigger error log)
        $sql = "SELECT * FROM users WHERE id IN (?)";
        $params = [0 => [1, 2, 3]]; // Numeric key

        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);

        // Should not modify SQL or params since numeric keys are skipped
        $this->assertEquals($sql, $newSql);
        $this->assertEquals([], $newParams);
    }

    public function testBuildInQueryWithEmptyArray()
    {
        // Define LODUR_TEST_SERVER constant to trigger error logging path
        if (!defined('LODUR_TEST_SERVER')) {
            define('LODUR_TEST_SERVER', true);
        }

        // Create mock for error_log function
        $errorLogMock = $this->getFunctionMock('Lodur\PicoPdo', 'error_log');
        $errorLogMock->expects($this->once())
            ->with($this->stringContains('Provided array for IN clause is empty or key is numeric'));

        // Test with empty array (should trigger error log)
        $sql = "SELECT * FROM users WHERE id IN (:ids)";
        $params = [':ids' => []]; // Empty array

        [$newSql, $newParams] = $this->_testBuildInQuery($sql, $params);

        // Should not modify SQL or params since empty arrays are skipped
        $this->assertEquals($sql, $newSql);
        $this->assertEquals([], $newParams);
    }

    public function testSelectOneWithEmptyResult()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM users WHERE `id` = :where_id LIMIT 1')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_id' => 999])
            ->willReturn(true);

        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(false); // No result found

        // Set PDO on trait instance
        $this->trait->setPdo($this->pdo);
        
        $result = $this->_testSelectOne('users', ['name', 'email'], ['id' => 999]);

        // Should return empty array when fetch returns false
        $this->assertEquals([], $result);
    }

    public function testSelectOneWithResult()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM users WHERE `id` = :where_id LIMIT 1')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_id' => 1])
            ->willReturn(true);

        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe', 'email' => 'john@example.com']);

        // Set PDO on trait instance
        $this->trait->setPdo($this->pdo);
        
        $result = $this->_testSelectOne('users', ['name', 'email'], ['id' => 1]);

        $this->assertEquals(['name' => 'John Doe', 'email' => 'john@example.com'], $result);
    }

    public function testSelectOneWithExtraQuerySuffix()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM users WHERE `id` = :where_id ORDER BY name LIMIT 1')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_id' => 1])
            ->willReturn(true);

        $this->pdoStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe', 'email' => 'john@example.com']);

        // Set PDO on trait instance
        $this->trait->setPdo($this->pdo);
        
        $result = $this->_testSelectOne('users', ['name', 'email'], ['id' => 1], null, 'ORDER BY name');

        $this->assertEquals(['name' => 'John Doe', 'email' => 'john@example.com'], $result);
    }

    public function testGetPdoDebugWithEmptyOutput()
    {
        // Mock PDOStatement that returns empty string from debugDumpParams
        $mockStmt = $this->createMock(PDOStatement::class);
        $mockStmt->expects($this->once())
            ->method('debugDumpParams')
            ->willReturnCallback(function() {
                // Don't output anything, simulating empty output
            });

        $result = $this->_testGetPdoDebug($mockStmt);

        // Should return empty string when ob_get_clean returns empty
        $this->assertEquals('', $result);
    }

    public function testBuildSqlClauseWithKeyValuePairs()
    {
        $data = ['name' => 'John', 'email' => 'john@example.com'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'set_');

        $this->assertEquals('`name` = :set_name, `email` = :set_email', $sql);
        $this->assertEquals([':set_name' => 'John', ':set_email' => 'john@example.com'], $params);
    }

    public function testBuildSqlClauseWithRawSql()
    {
        $data = ['name' => 'John', 'created_at = NOW()'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'set_');

        $this->assertStringContainsString('`name` = :set_name', $sql);
        $this->assertStringContainsString('created_at = NOW()', $sql);
        $this->assertEquals([':set_name' => 'John'], $params);
    }

    public function testBuildSqlClauseWithQuestionMarkPlaceholder()
    {
        $data = ['name' => 'John', 'last_login > ?' => '2024-01-01'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'update_');

        $this->assertStringContainsString('`name` = :update_name', $sql);
        $this->assertStringContainsString('last_login > :update_0', $sql);
        $this->assertEquals([':update_name' => 'John', ':update_0' => '2024-01-01'], $params);
    }

    public function testBuildSqlClauseWithCustomJoiner()
    {
        $data = ['name' => 'John', 'email' => 'john@example.com'];
        [$sql, $params] = $this->_testBuildSqlClause($data, 'where_', ' AND ');

        $this->assertEquals('`name` = :where_name AND `email` = :where_email', $sql);
        $this->assertEquals([':where_name' => 'John', ':where_email' => 'john@example.com'], $params);
    }

    public function testConvertToNamedPlaceholders()
    {
        $query = 'SELECT * FROM users WHERE id = ? AND name = ?';
        $bindings = [1, 'John'];
        [$newQuery, $newBindings] = $this->_testConvertToNamedPlaceholders($query, $bindings, 'where_');

        $this->assertEquals('SELECT * FROM users WHERE id = :where_0 AND name = :where_1', $newQuery);
        $this->assertEquals([':where_0' => 1, ':where_1' => 'John'], $newBindings);
    }

    public function testConvertToNamedPlaceholdersWithNoPlaceholders()
    {
        $query = 'SELECT * FROM users WHERE id = :id';
        $bindings = [':id' => 1];
        [$newQuery, $newBindings] = $this->_testConvertToNamedPlaceholders($query, $bindings, 'where_');

        $this->assertEquals('SELECT * FROM users WHERE id = :id', $newQuery);
        $this->assertEquals([':id' => 1], $newBindings);
    }

    public function testConvertToNamedPlaceholdersWithMixedPlaceholders()
    {
        $query = 'SELECT * FROM users WHERE id = :id AND name = ? AND status = ?';
        $bindings = [':id' => 1, 'John', 'active'];
        [$newQuery, $newBindings] = $this->_testConvertToNamedPlaceholders($query, $bindings, 'where_');

        $this->assertEquals('SELECT * FROM users WHERE id = :id AND name = :where_0 AND status = :where_1', $newQuery);
        $this->assertEquals([':id' => 1, ':where_0' => 'John', ':where_1' => 'active'], $newBindings);
    }

    // ========== selectJoin Tests ==========

    public function testSelectJoinBasic()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin('users u', null, 'LEFT JOIN profiles p ON p.user_id = u.id');
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithColumns()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT u.id, u.name, p.bio FROM users u LEFT JOIN profiles p ON p.user_id = u.id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            ['u.id', 'u.name', 'p.bio'],
            'LEFT JOIN profiles p ON p.user_id = u.id'
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithMultipleJoinsAsString()
    {
        $joins = "LEFT JOIN profiles p ON p.user_id = u.id LEFT JOIN addresses a ON a.user_id = u.id";
        
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id LEFT JOIN addresses a ON a.user_id = u.id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin('users u', null, $joins);
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithMultipleJoinsAsArray()
    {
        $joins = [
            'LEFT JOIN profiles p ON p.user_id = u.id',
            'LEFT JOIN addresses a ON a.user_id = u.id'
        ];
        
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id LEFT JOIN addresses a ON a.user_id = u.id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin('users u', null, $joins);
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithWhereKeyValue()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.id = :where_u_id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_u_id' => 1])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            ['u.id' => 1]
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithWhereArray()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.id = :where_u_id AND p.status = :where_p_status')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_u_id' => 1, ':where_p_status' => 'active'])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            ['u.id' => 1, 'p.status' => 'active']
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithWhereStringAndBindings()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.id = :where_0 AND p.status = :where_1')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 1, ':where_1' => 'active'])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            'u.id = ? AND p.status = ?',
            [1, 'active']
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithWhereStringAndNamedBindings()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.id = :id AND p.status = :status')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':id' => 1, ':status' => 'active'])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            'u.id = :id AND p.status = :status',
            [':id' => 1, ':status' => 'active']
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithSqlTail()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id ORDER BY u.name LIMIT 10')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            null,
            null,
            'ORDER BY u.name LIMIT 10'
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithWhereAndSqlTail()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT u.id, u.name FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.status = :where_u_status ORDER BY u.name LIMIT 10')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_u_status' => 'active'])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            ['u.id', 'u.name'],
            'LEFT JOIN profiles p ON p.user_id = u.id',
            ['u.status' => 'active'],
            null,
            'ORDER BY u.name LIMIT 10'
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithNullJoins()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u WHERE u.id = :where_u_id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_u_id' => 1])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            null,
            ['u.id' => 1]
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithEmptyJoins()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u WHERE u.id = :where_u_id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_u_id' => 1])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            '',
            ['u.id' => 1]
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithEmptyJoinsArray()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u WHERE u.id = :where_u_id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_u_id' => 1])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            [],
            ['u.id' => 1]
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithInClause()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.id IN (:ids0,:ids1,:ids2)')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':ids0' => 1, ':ids1' => 2, ':ids2' => 3])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            'u.id IN (:ids)',
            [':ids' => [1, 2, 3]]
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithQuestionMarkPlaceholderInWhere()
    {
        // When using ? placeholder with array, it gets converted to named placeholder
        // and then buildInQuery expands it into multiple placeholders
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.id IN (:where_00,:where_01,:where_02)')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_00' => 1, ':where_01' => 2, ':where_02' => 3])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            'u.id IN (?)',
            [[1, 2, 3]]
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithRawSqlInWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id WHERE u.created_at > :where_raw_0')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_raw_0' => '2024-01-01'])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            ['u.created_at > ?'],
            ['2024-01-01']
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithColumnString()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT u.id, u.name FROM users u LEFT JOIN profiles p ON p.user_id = u.id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            'u.id, u.name',
            'LEFT JOIN profiles p ON p.user_id = u.id'
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithNullWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            null
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithEmptyWhere()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            'LEFT JOIN profiles p ON p.user_id = u.id',
            []
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithBindingInJoinClause()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN orders o ON o.user_id = u.id AND o.total > :join_0 WHERE u.status = :where_u_status')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':join_0' => 100, ':where_u_status' => 'active'])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            [
                'LEFT JOIN orders o ON o.user_id = u.id AND o.total > ?' => 100
            ],
            ['u.status' => 'active']
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

    public function testSelectJoinWithMultipleBindingsInJoinClauses()
    {
        $this->pdo
            ->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM users u LEFT JOIN profiles p ON p.user_id = u.id AND p.verified = :join_0 LEFT JOIN orders o ON o.user_id = u.id AND o.total > :join_1 WHERE u.status = :where_u_status')
            ->willReturn($this->pdoStatement);

        $this->pdoStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':join_0' => 1, ':join_1' => 100, ':where_u_status' => 'active'])
            ->willReturn(true);

        $this->trait->setPdo($this->pdo);

        $stmt = $this->_testSelectJoin(
            'users u',
            null,
            [
                'LEFT JOIN profiles p ON p.user_id = u.id AND p.verified = ?' => 1,
                'LEFT JOIN orders o ON o.user_id = u.id AND o.total > ?' => 100
            ],
            ['u.status' => 'active']
        );
        $this->assertInstanceOf(PDOStatement::class, $stmt);
    }

}