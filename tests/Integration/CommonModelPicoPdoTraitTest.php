<?php

namespace Lodur\PicoPdo\Tests\Integration;

use PHPUnit\Framework\TestCase;
use PDO;

class CommonModelPicoPdoTraitTest extends TestCase
{
    private $trait;
    private $pdo;

    protected function setUp(): void
    {
        parent::setUp();
        
        // Create a real PDO connection
        $this->pdo = new PDO(
            "mysql:host=" . getenv('DB_HOST') . ";dbname=" . getenv('DB_NAME'),
            getenv('DB_USER'),
            getenv('DB_PASS'),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        
        // Create test table
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                status ENUM('active', 'pending', 'inactive') DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ");
        
        // Create a test class that uses the trait
        $testClass = new class($this->pdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                prepExec as public;
                exists as public;
                insert as public;
                insertReplace as public;
                insertIgnore as public;
                insertOnDuplicateKeyUpdate as public;
                update as public;
                delete as public;
                select as public;
                selectAll as public;
                buildWhereQuery as public;
                buildInQuery as public;
                getPdoDebug as public;
                removeInvalidColumns as public;
                resetTablesColumnsCache as public;
                normalizeTableName as public;
                getTableColumns as public;
            }
            
            public function __construct(PDO $pdo)
            {
                $this->pdo = $pdo;
            }
            public function resetTablesColumnsCache() {
                $this->tablesColumnsCache = [];
            }
        };
        
        $this->trait = $testClass;
    }

    protected function tearDown(): void
    {
        // Clean up test table
        $this->pdo->exec("DROP TABLE IF EXISTS test_users");
        $this->trait->resetTablesColumnsCache();
        parent::tearDown();
    }

    public function testInsertAndSelect()
    {
        // Test insert
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        
        $id = $this->trait->insert('test_users', $data);
        $this->assertGreaterThan(0, $id);
        
        // Test select
        $result = $this->trait->select('test_users', ['name', 'email'], 'id', $id);
        $this->assertEquals($data['name'], $result['name']);
        $this->assertEquals($data['email'], $result['email']);
    }

    public function testInsertWithDefaultValues()
    {
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        
        $id = $this->trait->insert('test_users', $data);
        
        // Verify default values
        $result = $this->trait->select('test_users', ['status', 'created_at'], 'id', $id);
        $this->assertEquals('active', $result['status']);
        $this->assertNotNull($result['created_at']);
    }

    public function testUpdate()
    {
        // Insert test data
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Update data
        $updateData = ['name' => 'Updated Name'];
        $affected = $this->trait->update('test_users', $updateData, 'id', $id);
        $this->assertEquals(1, $affected);
        
        // Verify update
        $result = $this->trait->select('test_users', ['name'], 'id', $id);
        $this->assertEquals('Updated Name', $result['name']);
    }

    public function testUpdateWithMultipleConditions()
    {
        // Insert test data
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Update with multiple conditions
        $updateData = ['status' => 'inactive'];
        $affected = $this->trait->update('test_users', $updateData, ['id' => $id, 'status' => 'active']);
        $this->assertEquals(1, $affected);
        
        // Verify update
        $result = $this->trait->select('test_users', ['status'], 'id', $id);
        $this->assertEquals('inactive', $result['status']);
    }

    public function testDelete()
    {
        // Insert test data
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Delete record
        $affected = $this->trait->delete('test_users', 'id', $id);
        $this->assertEquals(1, $affected);
        
        // Verify deletion
        $result = $this->trait->select('test_users', ['id'], 'id', $id);
        $this->assertEmpty($result);
    }

    public function testDeleteWithMultipleConditions()
    {
        // Insert test data
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Delete with multiple conditions
        $affected = $this->trait->delete('test_users', ['id' => $id, 'status' => 'active']);
        $this->assertEquals(1, $affected);
        
        // Verify deletion
        $result = $this->trait->select('test_users', ['id'], 'id', $id);
        $this->assertEmpty($result);
    }

    public function testExists()
    {
        // Insert test data
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test exists
        $this->assertTrue($this->trait->exists('test_users', 'id', $id));
        $this->assertFalse($this->trait->exists('test_users', 'id', 999));
    }

    public function testExistsWithMultipleConditions()
    {
        // Insert test data
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test exists with multiple conditions
        $this->assertTrue($this->trait->exists('test_users', ['id' => $id, 'status' => 'active']));
        $this->assertFalse($this->trait->exists('test_users', ['id' => $id, 'status' => 'inactive']));
    }

    public function testSelectAll()
    {
        // Insert multiple test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com'],
            ['name' => 'User 2', 'email' => 'user2@example.com'],
            ['name' => 'User 3', 'email' => 'user3@example.com']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test selectAll
        $results = $this->trait->selectAll('test_users', ['name', 'email']);
        $this->assertCount(3, $results);
        
        // Verify data
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 2', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectAllWithWhereCondition()
    {
        // Insert multiple test records with different statuses
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com', 'status' => 'active'],
            ['name' => 'User 2', 'email' => 'user2@example.com', 'status' => 'inactive'],
            ['name' => 'User 3', 'email' => 'user3@example.com', 'status' => 'active']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test selectAll with where condition
        $results = $this->trait->selectAll('test_users', ['name', 'email'], 'status', 'active');
        $this->assertCount(2, $results);
        
        // Verify data
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectAllWithCustomWhereCondition()
    {
        // Insert multiple test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com', 'status' => 'active'],
            ['name' => 'User 2', 'email' => 'user2@example.com', 'status' => 'inactive'],
            ['name' => 'User 3', 'email' => 'user3@example.com', 'status' => 'active']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test selectAll with custom where condition
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            'status = ?',
            ['active']
        );
        
        $this->assertCount(2, $results);
        
        // Verify data
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 3', $names);
    }

    public function testInsertIgnore()
    {
        $data = [
            'name' => 'User Ignore',
            'email' => 'ignore@example.com'
        ];
        $id1 = $this->trait->insertIgnore('test_users', $data);
        $this->assertGreaterThan(0, $id1);
        // Try to insert again with same email (should not duplicate due to unique constraint if present)
        $id2 = $this->trait->insertIgnore('test_users', $data);
        $this->assertNotEquals($id1, $id2); // Should insert again since no unique constraint on email
    }

    public function testInsertReplace()
    {
        $data = [
            'name' => 'User Replace',
            'email' => 'replace@example.com'
        ];
        $id1 = $this->trait->insert('test_users', $data);
        $this->assertGreaterThan(0, $id1);
        // Replace with same id
        $data2 = [
            'id' => $id1,
            'name' => 'User Replace Updated',
            'email' => 'replace@example.com'
        ];
        $id2 = $this->trait->insertReplace('test_users', $data2);
        $this->assertEquals($id1, $id2);
        $result = $this->trait->select('test_users', ['name'], 'id', $id1);
        $this->assertEquals('User Replace Updated', $result['name']);
    }

    public function testInsertOnDuplicateKeyUpdate()
    {
        $data = [
            'name' => 'User Dup',
            'email' => 'dup@example.com'
        ];
        $id1 = $this->trait->insert('test_users', $data);
        $this->assertGreaterThan(0, $id1);
        // Insert with duplicate id, update name
        $data2 = [
            'id' => $id1,
            'name' => 'User Dup Updated',
            'email' => 'dup@example.com'
        ];
        $id2 = $this->trait->insertOnDuplicateKeyUpdate('test_users', $data2, ['name' => 'User Dup Updated']);
        $this->assertEquals($id1, $id2);
        $result = $this->trait->select('test_users', ['name'], 'id', $id1);
        $this->assertEquals('User Dup Updated', $result['name']);
    }

    public function testBuildWhereQuery()
    {
        // Array input
        [$where, $params] = $this->trait->buildWhereQuery(['name' => 'A', 'status' => 'active']);
        $this->assertStringContainsString('`name` = :where_name', $where);
        $this->assertStringContainsString('`status` = :where_status', $where);
        $this->assertEquals([':where_name' => 'A', ':where_status' => 'active'], $params);
        // Raw string with bindings
        [$where2, $params2] = $this->trait->buildWhereQuery('name = ? AND status = ?', ['A', 'active']);
        $this->assertStringContainsString('name = :where_0', $where2);
        $this->assertStringContainsString('status = :where_1', $where2);
        $this->assertEquals([':where_0' => 'A', ':where_1' => 'active'], $params2);
    }

    public function testBuildInQuery()
    {
        $sql = 'SELECT * FROM test_users WHERE id IN (:ids)';
        $params = [':ids' => [1, 2, 3]];
        [$newSql, $newParams] = $this->trait->buildInQuery($sql, $params);
        $this->assertStringContainsString(':ids0', $newSql);
        $this->assertStringContainsString(':ids1', $newSql);
        $this->assertStringContainsString(':ids2', $newSql);
        $this->assertArrayHasKey(':ids0', $newParams);
        $this->assertArrayHasKey(':ids1', $newParams);
        $this->assertArrayHasKey(':ids2', $newParams);
    }

    public function testRemoveInvalidColumns()
    {
        $columns = ['id', 'name', 'email', 'not_a_column'];
        $filtered = $this->trait->removeInvalidColumns('test_users', $columns);
        $this->assertContains('id', $filtered);
        $this->assertContains('name', $filtered);
        $this->assertContains('email', $filtered);
        $this->assertNotContains('not_a_column', $filtered);
    }

    public function testResetTablesColumnsCache()
    {
        // Fill cache
        $this->trait->getTableColumns('test_users');
        $this->assertNotEmpty($this->trait->tablesColumnsCache);
        
        // Reset cache
        $this->trait->resetTablesColumnsCache();
        $this->assertEmpty($this->trait->tablesColumnsCache);
        
        // Verify cache is rebuilt after reset
        $columns = $this->trait->getTableColumns('test_users');
        $this->assertNotEmpty($columns);
        $this->assertNotEmpty($this->trait->tablesColumnsCache);
    }

    public function testNormalizeTableName()
    {
        $table = 'test_users; DROP TABLE users;';
        $normalized = $this->trait->normalizeTableName($table);
        $this->assertEquals('test_usersDROPTABLEusers', $normalized);
    }

    public function testGetTableColumns()
    {
        $columns = $this->trait->getTableColumns('test_users');
        $this->assertContains('id', $columns);
        $this->assertContains('name', $columns);
        $this->assertContains('email', $columns);
        $this->assertContains('status', $columns);
        $this->assertContains('created_at', $columns);
    }

    // --- Additional tests for 100% coverage ---

    /**
     * Test prepExec error handling by simulating a prepare failure.
     */
    public function testPrepExecPrepareFailure()
    {
        $mockPdo = $this->createMock(PDO::class);
        $mockPdo->method('prepare')->willReturn(false);
        $testClass = new class($mockPdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                prepExec as public;
                getPdoDebug as public;
            }
            public function __construct($pdo) { $this->pdo = $pdo; }
        };
        $this->expectException(\Error::class);
        $testClass->prepExec('SELECT 1');
    }

    /**
     * Test prepExec error handling by simulating an execute failure.
     */
    public function testPrepExecExecuteFailure()
    {
        $mockStmt = $this->createMock(\PDOStatement::class);
        $mockStmt->method('execute')->willThrowException(new \PDOException('Execute failed'));
        $mockPdo = $this->createMock(PDO::class);
        $mockPdo->method('prepare')->willReturn($mockStmt);
        $testClass = new class($mockPdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                prepExec as public;
                getPdoDebug as public;
            }
            public function __construct($pdo) { $this->pdo = $pdo; }
        };
        $this->expectException(\PDOException::class);
        $testClass->prepExec('SELECT 1');
    }

    /**
     * Test getPdoDebug with false statement.
     */
    public function testGetPdoDebugWithFalse()
    {
        $testClass = new class() {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                getPdoDebug as public;
            }
        };
        $result = $testClass->getPdoDebug(false);
        $this->assertEquals('Statement preparation failed', $result);
    }

    /**
     * Test buildInQuery with empty params and numeric key.
     */
    public function testBuildInQueryEdgeCases()
    {
        $testClass = new class() {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                buildInQuery as public;
            }
        };
        // Empty params
        [$sql, $params] = $testClass->buildInQuery('SELECT 1', []);
        $this->assertEquals('SELECT 1', $sql);
        $this->assertEquals([], $params);
        // Numeric key (should not expand)
        [$sql2, $params2] = $testClass->buildInQuery('SELECT * FROM t WHERE id IN (?)', [0 => [1,2,3]]);
        $this->assertEquals('SELECT * FROM t WHERE id IN (?)', $sql2);
    }

    /**
     * Test buildWhereQuery with empty input and with no bindings.
     */
    public function testBuildWhereQueryEdgeCases()
    {
        $testClass = new class() {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                buildWhereQuery as public;
            }
        };
        [$where, $params] = $testClass->buildWhereQuery();
        $this->assertEquals('', $where);
        $this->assertEquals([], $params);
        [$where2, $params2] = $testClass->buildWhereQuery('created_at IS NULL');
        $this->assertEquals('created_at IS NULL', $where2);
        $this->assertEquals([], $params2);
    }

    /**
     * Test LRU cache eviction in getTableColumns.
     */
    public function testGetTableColumnsLRUEviction()
    {
        $testClass = new class($this->pdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                getTableColumns as public;
            }
            public function __construct($pdo) { $this->pdo = $pdo; }
        };
        // Create many tables to fill the cache
        for ($i = 0; $i < 105; $i++) {
            $table = "test_lru_$i";
            $this->pdo->exec("CREATE TABLE IF NOT EXISTS $table (id INT PRIMARY KEY)");
            $testClass->getTableColumns($table);
        }
        $this->assertLessThanOrEqual(100, count($testClass->tablesColumnsCache));
        // Cleanup
        for ($i = 0; $i < 105; $i++) {
            $table = "test_lru_$i";
            $this->pdo->exec("DROP TABLE IF EXISTS $table");
        }
    }

    /**
     * Test getPdoDebug with a real PDOStatement.
     */
    public function testGetPdoDebugWithRealStatement()
    {
        $testClass = new class($this->pdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                getPdoDebug as public;
                prepExec as public;
            }
            public function __construct($pdo) { $this->pdo = $pdo; }
        };
        
        $stmt = $testClass->prepExec('SELECT 1');
        $debug = $testClass->getPdoDebug($stmt);
        $this->assertNotEmpty($debug);
        $this->assertStringContainsString('SELECT 1', $debug);
    }

    /**
     * Test buildWhereQuery with a raw SQL condition containing a named placeholder and a non-array binding.
     */
    public function testBuildWhereQueryWithNamedPlaceholderAndNonArrayBinding()
    {
        $testClass = new class($this->pdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                buildWhereQuery as public;
            }
            public function __construct($pdo) { $this->pdo = $pdo; }
        };
        $where = "id = :id";
        $bindings = 1;
        [$whereClause, $params] = $testClass->buildWhereQuery($where, $bindings);
        $this->assertEquals($where, $whereClause);
        $this->assertEquals([$bindings], $params);
    }

    /**
     * Test buildWhereQuery with a raw SQL condition and an array binding containing an array element.
     */
    public function testBuildWhereQueryWithArrayBindingContainingArray()
    {
        $testClass = new class($this->pdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                buildWhereQuery as public;
            }
            public function __construct($pdo) { $this->pdo = $pdo; }
        };
        $where = "id IN (:ids)";
        $bindings = [':ids' => [1, 2, 3]];
        [$whereClause, $params] = $testClass->buildWhereQuery($where, $bindings);
        $this->assertStringContainsString('IN', $whereClause);
        $this->assertNotEmpty($params);
    }

    /**
     * Test prepExec with a SQL statement and an array of parameters containing an array element.
     */
    public function testPrepExecWithArrayParamsContainingArray()
    {
        $testClass = new class($this->pdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                prepExec as public;
            }
            public function __construct($pdo) { $this->pdo = $pdo; }
        };
        $sql = "SELECT * FROM test_users WHERE id IN (:ids)";
        $params = [':ids' => [1, 2, 3]];
        $stmt = $testClass->prepExec($sql, $params);
        $this->assertInstanceOf(\PDOStatement::class, $stmt);
    }
} 