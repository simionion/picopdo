<?php

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PHPUnit\Framework\TestCase;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\MockObject\MockObject;
use Error;

/**
 * Test class that uses the trait to access protected methods
 */
class CommonModelPicoPdoTraitTest extends TestCase
{
    use CommonModelPicoPdoTrait {
        prepExec as public _testPrepExec;
        buildWhereQuery as public _testBuildWhereQuery;
        exists as public _testExists;
        insert as public _testInsert;
        update as public _testUpdate;
        select as public _testSelect;
        selectAll as public _testSelectAll;
        delete as public _testDelete;
        buildInQuery as public _testBuildInQuery;
    }

    private MockObject $mockPdo;
    private MockObject $mockStatement;

    protected function setUp(): void
    {
        parent::setUp();
        $this->mockPdo = $this->createMock(PDO::class);
        $this->mockStatement = $this->createMock(PDOStatement::class);
        $this->pdo = $this->mockPdo;
    }

    /**
     * Test prepExec with simple positional placeholder (from PHPDoc example)
     */
    public function testPrepExecWithSimplePositionalPlaceholder()
    {
        $sql = "SELECT * FROM users WHERE id = ?";
        $params = [5];
        
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with($params)
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
        
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
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
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with($newSql)
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with($newParams)
            ->willReturn(true);
            
        $result = $this->_testPrepExec($newSql, $newParams);
        
        $this->assertInstanceOf(PDOStatement::class, $result);
    }

    public function testPrepExecWithEmptySql()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with('')
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
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
        
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with($params)
            ->willReturn(false);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('errorInfo')
            ->willReturn(['00000', '', 'Test error']);
            
        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('Failed to execute query: Test error');
        
        $this->_testPrepExec($sql, $params);
    }

    public function testPrepExecWithFailedPrepare()
    {
        $sql = "SELECT * FROM users WHERE id = :id";
        
        $this->mockPdo
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
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM `users` WHERE `id` = :where_id LIMIT 1")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_id' => 1])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);
            
        $result = $this->_testExists('users', 'id', 1);
        $this->assertTrue($result);
    }

    /**
     * Test exists with associative array (from PHPDoc example)
     */
    public function testExistsWithAssociativeArray()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM `users` WHERE `status` = :where_status AND `email_verified` = :where_email_verified LIMIT 1")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_status' => 'active', ':where_email_verified' => 1])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);
            
        $result = $this->_testExists('users', ['status' => 'active', 'email_verified' => 1]);
        $this->assertTrue($result);
    }

    /**
     * Test exists with custom WHERE clause (from PHPDoc example)
     */
    public function testExistsWithCustomWhereClause()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM `users` WHERE email = :where_0 AND created_at > :where_1 LIMIT 1")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 'user@example.com', ':where_1' => '2024-01-01'])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);
            
        $result = $this->_testExists('users', 'email = ? AND created_at > ?', ['user@example.com', '2024-01-01']);
        $this->assertTrue($result);
    }

    /**
     * Test exists with null where
     */
    public function testExistsWithNullWhere()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM `users`  LIMIT 1")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);
            
        $result = $this->_testExists('users', null);
        $this->assertTrue($result);
    }

    /**
     * Test exists with empty array where
     */
    public function testExistsWithEmptyArrayWhere()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT 1 as `true` FROM `users`  LIMIT 1")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('rowCount')
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

        $this->assertEquals('`id` = :where_id', $whereStr);
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
        
        // Should not modify SQL or params since numeric keys are not supported
        $this->assertEquals($sql, $newSql);
        $this->assertEquals($params, $newParams);
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
        
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
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
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->mockStatement);
            
        // Test that the parameters are executed with expanded values
        $this->mockStatement
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
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with($expectedSql)
            ->willReturn($this->mockStatement);
            
        // Test that the parameters are executed with expanded values
        $this->mockStatement
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
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM `users`  LIMIT 1")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe', 'email' => 'john@example.com']);
            
        $result = $this->_testSelect('users', ['name', 'email'], null);
        $this->assertEquals(['name' => 'John Doe', 'email' => 'john@example.com'], $result);
    }

    public function testSelectWithEmptyArrayWhere()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM `users`  LIMIT 1")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe', 'email' => 'john@example.com']);
            
        $result = $this->_testSelect('users', ['name', 'email'], []);
        $this->assertEquals(['name' => 'John Doe', 'email' => 'john@example.com'], $result);
    }

    public function testSelectAllWithNullWhere()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM `users`")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['name' => 'John Doe', 'email' => 'john@example.com'],
                ['name' => 'Jane Smith', 'email' => 'jane@example.com']
            ]);
            
        $result = $this->_testSelectAll('users', ['name', 'email'], null);
        $this->assertEquals([
            ['name' => 'John Doe', 'email' => 'john@example.com'],
            ['name' => 'Jane Smith', 'email' => 'jane@example.com']
        ], $result);
    }

    public function testSelectAllWithEmptyArrayWhere()
    {
        $this->mockPdo
            ->expects($this->once())
            ->method('prepare')
            ->with("SELECT name, email FROM `users`")
            ->willReturn($this->mockStatement);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('execute')
            ->with([])
            ->willReturn(true);
            
        $this->mockStatement
            ->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['name' => 'John Doe', 'email' => 'john@example.com'],
                ['name' => 'Jane Smith', 'email' => 'jane@example.com']
            ]);
            
        $result = $this->_testSelectAll('users', ['name', 'email'], []);
        $this->assertEquals([
            ['name' => 'John Doe', 'email' => 'john@example.com'],
            ['name' => 'Jane Smith', 'email' => 'jane@example.com']
        ], $result);
    }
}