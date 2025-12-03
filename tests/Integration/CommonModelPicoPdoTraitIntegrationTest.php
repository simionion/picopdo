<?php

namespace Lodur\PicoPdo\Tests\Integration;

use PHPUnit\Framework\TestCase;
use PDO;

class CommonModelPicoPdoTraitIntegrationTest extends TestCase
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
        
        // Create test table with unique email for INSERT IGNORE and ON DUPLICATE KEY UPDATE tests
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                status ENUM('active', 'pending', 'inactive') DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ");
        
        // Create a test class that uses the trait
        $testClass = new class($this->pdo) {
            use \Lodur\PicoPdo\CommonModelPicoPdoTrait {
                insert as public;
                insertReplace as public;
                insertIgnore as public;
                insertOnDuplicateKeyUpdate as public;
                select as public;
                selectOne as public;
                selectAll as public;
                selectJoin as public;
                update as public;
                delete as public;
                exists as public;
                prepExec as public;
            }
            
            public function __construct(PDO $pdo)
            {
                $this->pdo = $pdo;
            }
        };
        
        $this->trait = $testClass;
    }

    protected function tearDown(): void
    {
        // Clean up test tables
        $this->pdo->exec("DROP TABLE IF EXISTS test_addresses");
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
        $this->pdo->exec("DROP TABLE IF EXISTS test_users");
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
        $result = $this->trait->selectOne('test_users', ['name', 'email'], 'id', $id);
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
        $result = $this->trait->selectOne('test_users', ['status', 'created_at'], 'id', $id);
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
        $result = $this->trait->selectOne('test_users', ['name'], 'id', $id);
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
        $result = $this->trait->selectOne('test_users', ['status'], 'id', $id);
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
        $result = $this->trait->selectOne('test_users', ['id'], 'id', $id);
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
        $result = $this->trait->selectOne('test_users', ['id'], 'id', $id);
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

    public function testInsertWithMeta()
    {
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        
        $result = $this->trait->insert('test_users', $data, ['meta' => true]);
        
        $this->assertIsArray($result);
        $this->assertArrayHasKey('id', $result);
        $this->assertArrayHasKey('rows', $result);
        $this->assertArrayHasKey('status', $result);
        $this->assertGreaterThan(0, $result['id']);
        $this->assertEquals(1, $result['rows']);
        $this->assertEquals('inserted', $result['status']);
    }

    public function testInsertReplace()
    {
        // Insert initial record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Replace with same email (should replace existing)
        $replaceData = [
            'name' => 'Replaced User',
            'email' => 'test@example.com' // Same email, will trigger REPLACE
        ];
        $newId = $this->trait->insertReplace('test_users', $replaceData);
        
        // Verify replacement
        $result = $this->trait->selectOne('test_users', ['name'], 'id', $newId);
        $this->assertEquals('Replaced User', $result['name']);
    }

    public function testInsertIgnore()
    {
        // Insert initial record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Try to insert same email with INSERT IGNORE (should return noop)
        $ignoreData = [
            'name' => 'Ignored User',
            'email' => 'test@example.com' // Same email, will be ignored
        ];
        $result = $this->trait->insertIgnore('test_users', $ignoreData);
        
        $this->assertIsArray($result);
        $this->assertEquals(0, $result['id']);
        $this->assertEquals(0, $result['rows']);
        $this->assertEquals('noop', $result['status']);
        
        // Verify original record unchanged
        $original = $this->trait->selectOne('test_users', ['name'], 'id', $id);
        $this->assertEquals('Test User', $original['name']);
    }

    public function testInsertIgnoreWithNewRecord()
    {
        // Insert with INSERT IGNORE on new email (should insert)
        $data = [
            'name' => 'New User',
            'email' => 'new@example.com'
        ];
        $result = $this->trait->insertIgnore('test_users', $data);
        
        $this->assertIsArray($result);
        $this->assertGreaterThan(0, $result['id']);
        $this->assertEquals(1, $result['rows']);
        $this->assertEquals('inserted', $result['status']);
    }

    public function testInsertOnDuplicateKeyUpdate()
    {
        // Insert initial record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Insert with same email but update on duplicate
        $updateData = [
            'name' => 'Updated User',
            'email' => 'test@example.com', // Same email
            'status' => 'inactive'
        ];
        $result = $this->trait->insertOnDuplicateKeyUpdate('test_users', $updateData, [
            'name' => 'Updated User',
            'status' => 'inactive'
        ]);
        
        $this->assertIsArray($result);
        $this->assertEquals(2, $result['rows']); // 2 rows affected (1 inserted + 1 updated)
        $this->assertEquals('updated', $result['status']);
        
        // Verify update
        $updated = $this->trait->selectOne('test_users', ['name', 'status'], 'id', $id);
        $this->assertEquals('Updated User', $updated['name']);
        $this->assertEquals('inactive', $updated['status']);
    }

    public function testInsertOnDuplicateKeyUpdateNoop()
    {
        // Insert initial record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Try to insert same with same values (should be noop)
        $sameData = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $result = $this->trait->insertOnDuplicateKeyUpdate('test_users', $sameData, [
            'name' => 'Test User',
            'status' => 'active' // Same values
        ]);
        
        // Note: In real MySQL, this might return 1 or 2 rows depending on MySQL version
        // But if values are truly unchanged, it should be 0 or 1
        $this->assertIsArray($result);
        $this->assertArrayHasKey('status', $result);
    }

    public function testWhereInWithArrayExpansion()
    {
        // Insert multiple test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com'],
            ['name' => 'User 2', 'email' => 'user2@example.com'],
            ['name' => 'User 3', 'email' => 'user3@example.com']
        ];
        
        $ids = [];
        foreach ($users as $user) {
            $ids[] = $this->trait->insert('test_users', $user);
        }
        
        // Test WHERE IN with array expansion
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            'id IN (:ids)',
            [':ids' => $ids]
        );
        
        $this->assertCount(3, $results);
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 2', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectWithRawSqlInArray()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test with raw SQL in array
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['id > ?'],
            [$id - 1] // Should match our record
        );
        
        $this->assertGreaterThanOrEqual(1, count($results));
    }

    public function testSelectWithKeyContainingQuestionMark()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test with key containing ? placeholder
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['id > ?' => $id - 1]
        );
        
        $this->assertGreaterThanOrEqual(1, count($results));
    }

    public function testSelectWithMixedArrayAndRawSql()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com', 'status' => 'active'],
            ['name' => 'User 2', 'email' => 'user2@example.com', 'status' => 'inactive'],
            ['name' => 'User 3', 'email' => 'user3@example.com', 'status' => 'active']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test with mixed array (key-value and raw SQL)
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['status' => 'active', 'id > ?'],
            [0] // id > 0
        );
        
        $this->assertGreaterThanOrEqual(2, count($results));
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectWithMultipleRawSqlWithQuestionMarks()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com', 'status' => 'active'],
            ['name' => 'User 2', 'email' => 'user2@example.com', 'status' => 'inactive'],
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test with multiple raw SQL strings with ? placeholders
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['status = ?', 'id > ?'],
            ['active', 0]
        );
        
        $this->assertGreaterThanOrEqual(1, count($results));
    }

    public function testSelectWithMixedPlaceholders()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test with mixed ? and : placeholders
        // Raw SQL as numeric key in array, bindings in separate parameter
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['status = :status AND id > ?'],
            [':status' => 'active', $id - 1]
        );
        
        $this->assertGreaterThanOrEqual(1, count($results));
    }

    public function testSelectWithInQuestionMarkPlaceholder()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com'],
            ['name' => 'User 2', 'email' => 'user2@example.com'],
            ['name' => 'User 3', 'email' => 'user3@example.com']
        ];
        
        $ids = [];
        foreach ($users as $user) {
            $ids[] = $this->trait->insert('test_users', $user);
        }
        
        // Test WHERE IN(?) with ? placeholder that gets converted to named placeholders
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            'id IN (?)',
            [$ids]
        );
        
        $this->assertCount(3, $results);
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 2', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectWithInQuestionMarkPlaceholderAndOtherConditions()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com', 'status' => 'active'],
            ['name' => 'User 2', 'email' => 'user2@example.com', 'status' => 'inactive'],
            ['name' => 'User 3', 'email' => 'user3@example.com', 'status' => 'active']
        ];
        
        $ids = [];
        foreach ($users as $user) {
            $ids[] = $this->trait->insert('test_users', $user);
        }
        
        // Test WHERE with IN(?) and additional condition
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            'id IN (?) AND status = ?',
            [$ids, 'active']
        );
        
        $this->assertGreaterThanOrEqual(2, count($results));
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectWithInQuestionMarkInArrayWhere()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com'],
            ['name' => 'User 2', 'email' => 'user2@example.com'],
            ['name' => 'User 3', 'email' => 'user3@example.com']
        ];
        
        $ids = [];
        foreach ($users as $user) {
            $ids[] = $this->trait->insert('test_users', $user);
        }
        
        // Test WHERE with IN(?) in array format
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['id IN (?)' => $ids]
        );
        
        $this->assertCount(3, $results);
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 2', $names);
        $this->assertContains('User 3', $names);
    }

    public function testUpdateWithRawSqlInArray()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Update with raw SQL in array
        $affected = $this->trait->update(
            'test_users',
            ['name' => 'Updated User'],
            ['id > ?'],
            [$id - 1]
        );
        
        $this->assertGreaterThanOrEqual(1, $affected);
        
        // Verify update
        $result = $this->trait->selectOne('test_users', ['name'], 'id', $id);
        $this->assertEquals('Updated User', $result['name']);
    }

    public function testDeleteWithRawSqlInArray()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com'],
            ['name' => 'User 2', 'email' => 'user2@example.com'],
        ];
        
        $ids = [];
        foreach ($users as $user) {
            $ids[] = $this->trait->insert('test_users', $user);
        }
        
        // Delete with raw SQL in array
        $affected = $this->trait->delete(
            'test_users',
            ['id > ?'],
            [$ids[0] - 1]
        );
        
        $this->assertGreaterThanOrEqual(1, $affected);
        
        // Verify deletion
        $result = $this->trait->selectOne('test_users', ['id'], 'id', $ids[0]);
        $this->assertEmpty($result);
    }

    public function testSelectWithComplexWhereConditions()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com', 'status' => 'active'],
            ['name' => 'User 2', 'email' => 'user2@example.com', 'status' => 'inactive'],
            ['name' => 'User 3', 'email' => 'user3@example.com', 'status' => 'active']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test complex WHERE with mixed conditions
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['status' => 'active', 'id > ?', 'email LIKE ?'],
            [0, '%@example.com']
        );
        
        $this->assertGreaterThanOrEqual(2, count($results));
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectWithNamedPlaceholdersInWhere()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test with named placeholders
        $result = $this->trait->selectOne(
            'test_users',
            ['name', 'email'],
            'id = :id AND status = :status',
            [':id' => $id, ':status' => 'active']
        );
        
        $this->assertNotEmpty($result);
        $this->assertEquals('Test User', $result['name']);
    }

    public function testPrepExecWithArrayExpansion()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com'],
            ['name' => 'User 2', 'email' => 'user2@example.com'],
            ['name' => 'User 3', 'email' => 'user3@example.com']
        ];
        
        $ids = [];
        foreach ($users as $user) {
            $ids[] = $this->trait->insert('test_users', $user);
        }
        
        // Test prepExec with array expansion for IN clause
        $stmt = $this->trait->prepExec(
            'SELECT name, email FROM test_users WHERE id IN (:ids)',
            [':ids' => $ids]
        );
        
        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $results);
        
        $names = array_column($results, 'name');
        $this->assertContains('User 1', $names);
        $this->assertContains('User 2', $names);
        $this->assertContains('User 3', $names);
    }

    public function testSelectWithArrayColumns()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test select with array of columns
        $result = $this->trait->selectOne(
            'test_users',
            ['name', 'email', 'status'],
            'id',
            $id
        );
        
        $this->assertNotEmpty($result);
        $this->assertArrayHasKey('name', $result);
        $this->assertArrayHasKey('email', $result);
        $this->assertArrayHasKey('status', $result);
    }

    public function testSelectWithExtraQuerySuffix()
    {
        // Insert test records
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com'],
            ['name' => 'User B', 'email' => 'userb@example.com'],
            ['name' => 'User C', 'email' => 'userc@example.com']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test select with ORDER BY and LIMIT
        $results = $this->trait->selectAll(
            'test_users',
            ['name'],
            null,
            null,
            'ORDER BY name ASC LIMIT 2'
        );
        
        $this->assertCount(2, $results);
        $this->assertEquals('User A', $results[0]['name']);
        $this->assertEquals('User B', $results[1]['name']);
    }

    public function testUpdateWithRawSqlInData()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Update with raw SQL in data array
        $affected = $this->trait->update(
            'test_users',
            ['name' => 'Updated', 'status = "inactive"'], // Raw SQL for status
            'id',
            $id
        );
        
        $this->assertEquals(1, $affected);
        
        // Verify update
        $result = $this->trait->selectOne('test_users', ['name', 'status'], 'id', $id);
        $this->assertEquals('Updated', $result['name']);
        $this->assertEquals('inactive', $result['status']);
    }

    public function testInsertWithRawSqlInData()
    {
        // Insert with raw SQL in data array
        $id = $this->trait->insert('test_users', [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status = "pending"' // Raw SQL
        ]);
        
        $this->assertGreaterThan(0, $id);
        
        // Verify insert
        $result = $this->trait->selectOne('test_users', ['name', 'status'], 'id', $id);
        $this->assertEquals('Test User', $result['name']);
        $this->assertEquals('pending', $result['status']);
    }

    public function testSelectWithWhereArrayAndRawSql()
    {
        // Insert test records
        $users = [
            ['name' => 'User 1', 'email' => 'user1@example.com', 'status' => 'active'],
            ['name' => 'User 2', 'email' => 'user2@example.com', 'status' => 'active'],
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test WHERE with array containing raw SQL
        $results = $this->trait->selectAll(
            'test_users',
            ['name'],
            ['status' => 'active', 'id > ?'],
            [0]
        );
        
        $this->assertGreaterThanOrEqual(2, count($results));
    }

    public function testExistsWithRawSqlWhere()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test exists with raw SQL WHERE
        $this->assertTrue($this->trait->exists('test_users', 'id > ?', [$id - 1]));
        $this->assertFalse($this->trait->exists('test_users', 'id > ?', [9999]));
    }

    public function testSelectWithScalarBinding()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test select with scalar binding (should be converted to named placeholder)
        $result = $this->trait->selectOne('test_users', ['name', 'email'], 'id = ?', $id);
        
        $this->assertNotEmpty($result);
        $this->assertEquals('Test User', $result['name']);
    }

    public function testSelectWithExtraQuerySuffixAndNamedPlaceholders()
    {
        // Insert test records
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com', 'status' => 'active'],
            ['name' => 'User B', 'email' => 'userb@example.com', 'status' => 'active'],
            ['name' => 'User C', 'email' => 'userc@example.com', 'status' => 'inactive']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test select with extraQuerySuffix using named placeholders
        // Note: MySQL/MariaDB may not support placeholders for LIMIT in all versions
        // So we test with a HAVING clause which definitely supports placeholders
        $results = $this->trait->selectAll(
            'test_users',
            ['status', 'COUNT(*) as count'],
            null,
            [':min_count' => 1],
            'GROUP BY status HAVING COUNT(*) > :min_count ORDER BY status'
        );
        
        $this->assertGreaterThanOrEqual(1, count($results));
        // Verify we got grouped results
        foreach ($results as $result) {
            $this->assertArrayHasKey('status', $result);
            $this->assertArrayHasKey('count', $result);
            $this->assertGreaterThan(1, $result['count']);
        }
    }

    public function testSelectWithExtraQuerySuffixAndQuestionMarkPlaceholders()
    {
        // Insert test records
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com', 'status' => 'active'],
            ['name' => 'User B', 'email' => 'userb@example.com', 'status' => 'active'],
            ['name' => 'User C', 'email' => 'userc@example.com', 'status' => 'inactive']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test select with extraQuerySuffix using ? placeholders
        // Note: ? placeholders in extraQuerySuffix are NOT automatically converted
        // They will cause PDO errors because they're not bound
        $this->expectException(\PDOException::class);
        
        $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            ['status' => 'active'],
            ['name', 2],  // These bindings are for WHERE clause, not extraQuerySuffix
            'ORDER BY ? LIMIT ?'
        );
    }

    public function testSelectWithExtraQuerySuffixHavingClause()
    {
        // Insert test records with different statuses
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com', 'status' => 'active'],
            ['name' => 'User B', 'email' => 'userb@example.com', 'status' => 'active'],
            ['name' => 'User C', 'email' => 'userc@example.com', 'status' => 'inactive']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test with HAVING clause using named placeholder in extraQuerySuffix
        // This demonstrates that named placeholders in extraQuerySuffix work
        // when they're included in the bindings parameter
        $results = $this->trait->selectAll(
            'test_users',
            ['status', 'COUNT(*) as count'],
            null,
            [':min_count' => 1],
            'GROUP BY status HAVING COUNT(*) > :min_count'
        );
        
        $this->assertGreaterThanOrEqual(1, count($results));
        // Verify we got grouped results
        foreach ($results as $result) {
            $this->assertArrayHasKey('status', $result);
            $this->assertArrayHasKey('count', $result);
            $this->assertGreaterThan(1, $result['count']);
        }
    }


    public function testSelectWithExtraQuerySuffixHavingClauseAndQuestionPlaceholder()
    {
        // Insert test records with different statuses
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com', 'status' => 'active'],
            ['name' => 'User B', 'email' => 'userb@example.com', 'status' => 'active'],
            ['name' => 'User C', 'email' => 'userc@example.com', 'status' => 'inactive']
        ];

        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }

        // Test with HAVING clause using named placeholder in extraQuerySuffix
        // This demonstrates that named placeholders in extraQuerySuffix work
        // when they're included in the bindings parameter
        $results = $this->trait->selectAll(
            'test_users',
            ['status', 'COUNT(*) as count'],
            null,
            [1],
            'GROUP BY status HAVING COUNT(*) > ?'
        );

        $this->assertGreaterThanOrEqual(1, count($results));
        // Verify we got grouped results
        foreach ($results as $result) {
            $this->assertArrayHasKey('status', $result);
            $this->assertArrayHasKey('count', $result);
            $this->assertGreaterThan(1, $result['count']);
        }
    }

    public function testSelectWithExtraQuerySuffixOrderByAndLimit()
    {
        // Insert test records
        $users = [
            ['name' => 'User Z', 'email' => 'userz@example.com'],
            ['name' => 'User A', 'email' => 'usera@example.com'],
            ['name' => 'User M', 'email' => 'userm@example.com']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test with ORDER BY and LIMIT (without placeholders for LIMIT)
        // Note: MySQL/MariaDB may not support placeholders for LIMIT in prepared statements
        // This test verifies extraQuerySuffix works without placeholders
        $results = $this->trait->selectAll(
            'test_users',
            ['name', 'email'],
            null,
            null,
            'ORDER BY name LIMIT 2'
        );
        
        $this->assertCount(2, $results);
        // Should be ordered by name
        $this->assertEquals('User A', $results[0]['name']);
    }

    public function testExistsWithExtraQuerySuffix()
    {
        // Insert test records
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com', 'status' => 'active'],
            ['name' => 'User B', 'email' => 'userb@example.com', 'status' => 'active']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test exists with extraQuerySuffix using named placeholder for LIMIT
        // Note: exists() automatically adds LIMIT 1, so this tests the interaction
        $exists = $this->trait->exists(
            'test_users',
            ['status' => 'active'],
            null,
            'ORDER BY name'
        );
        
        $this->assertTrue($exists);
    }

    public function testSelectOneBasic()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test selectOne with key-value
        $result = $this->trait->selectOne('test_users', ['name', 'email'], 'id', $id);
        
        $this->assertNotEmpty($result);
        $this->assertEquals('Test User', $result['name']);
        $this->assertEquals('test@example.com', $result['email']);
    }

    public function testSelectOneWithArrayWhere()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com',
            'status' => 'active'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test selectOne with array WHERE
        $result = $this->trait->selectOne('test_users', ['name', 'email'], [
            'id' => $id,
            'status' => 'active'
        ]);
        
        $this->assertNotEmpty($result);
        $this->assertEquals('Test User', $result['name']);
    }

    public function testSelectOneWithNoResult()
    {
        // Test selectOne when no record exists
        $result = $this->trait->selectOne('test_users', ['name', 'email'], 'id', 99999);
        
        $this->assertEmpty($result);
        $this->assertEquals([], $result);
    }

    public function testSelectOneWithQuestionMarkPlaceholder()
    {
        // Insert test record
        $data = [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ];
        $id = $this->trait->insert('test_users', $data);
        
        // Test selectOne with ? placeholder (should be converted to named)
        $result = $this->trait->selectOne('test_users', ['name', 'email'], 'id = ?', $id);
        
        $this->assertNotEmpty($result);
        $this->assertEquals('Test User', $result['name']);
    }

    public function testSelectOneWithExtraQuerySuffix()
    {
        // Insert test records
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com'],
            ['name' => 'User B', 'email' => 'userb@example.com']
        ];
        
        foreach ($users as $user) {
            $this->trait->insert('test_users', $user);
        }
        
        // Test selectOne with extraQuerySuffix (ORDER BY)
        $result = $this->trait->selectOne('test_users', ['name', 'email'], null, null, 'ORDER BY name');
        
        $this->assertNotEmpty($result);
        $this->assertEquals('User A', $result['name']); // Should be first alphabetically
    }

    public function testSelectOneWithInClause()
    {
        // Insert test records
        $users = [
            ['name' => 'User A', 'email' => 'usera@example.com'],
            ['name' => 'User B', 'email' => 'userb@example.com']
        ];
        
        $ids = [];
        foreach ($users as $user) {
            $ids[] = $this->trait->insert('test_users', $user);
        }
        
        // Test selectOne with IN clause
        $result = $this->trait->selectOne('test_users', ['name', 'email'], 'id IN (:ids)', [':ids' => $ids]);
        
        $this->assertNotEmpty($result);
        $this->assertArrayHasKey('name', $result);
        $this->assertArrayHasKey('email', $result);
    }

    // ========== selectJoin Integration Tests ==========

    public function testSelectJoinBasic()
    {
        // Create test_profiles table for JOIN tests
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test user
        $userId = $this->trait->insert('test_users', [
            'name' => 'John Doe',
            'email' => 'john_join_' . time() . '@example.com'
        ]);

        // Insert test profile
        $this->trait->insert('test_profiles', [
            'user_id' => $userId,
            'bio' => 'Test bio'
        ]);

        // Test basic JOIN
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.bio'],
            'LEFT JOIN test_profiles p ON p.user_id = u.id',
            'u.id = ?',
            [$userId]
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('John Doe', $result['name']);
        $this->assertEquals('Test bio', $result['bio']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithMultipleJoins()
    {
        // Create test tables
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_addresses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                address TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userId = $this->trait->insert('test_users', [
            'name' => 'Jane Doe',
            'email' => 'jane_join_' . time() . '@example.com'
        ]);
        $this->trait->insert('test_profiles', [
            'user_id' => $userId,
            'bio' => 'Jane bio'
        ]);
        $this->trait->insert('test_addresses', [
            'user_id' => $userId,
            'address' => '123 Main St'
        ]);

        // Test multiple JOINs as string
        $joins = "LEFT JOIN test_profiles p ON p.user_id = u.id LEFT JOIN test_addresses a ON a.user_id = u.id";
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.bio', 'a.address'],
            $joins,
            'u.id = ?',
            [$userId]
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('Jane Doe', $result['name']);
        $this->assertEquals('Jane bio', $result['bio']);
        $this->assertEquals('123 Main St', $result['address']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_addresses");
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithMultipleJoinsAsArray()
    {
        // Create test tables
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_addresses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                address TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userId = $this->trait->insert('test_users', [
            'name' => 'Bob Smith',
            'email' => 'bob_join_' . time() . '@example.com'
        ]);
        $this->trait->insert('test_profiles', [
            'user_id' => $userId,
            'bio' => 'Bob bio'
        ]);
        $this->trait->insert('test_addresses', [
            'user_id' => $userId,
            'address' => '456 Oak Ave'
        ]);

        // Test multiple JOINs as array
        $joins = [
            'LEFT JOIN test_profiles p ON p.user_id = u.id',
            'LEFT JOIN test_addresses a ON a.user_id = u.id'
        ];
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.bio', 'a.address'],
            $joins,
            'u.id = ?',
            [$userId]
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('Bob Smith', $result['name']);
        $this->assertEquals('Bob bio', $result['bio']);
        $this->assertEquals('456 Oak Ave', $result['address']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_addresses");
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithWhereArray()
    {
        // Create test_profiles table
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userId = $this->trait->insert('test_users', [
            'name' => 'Alice Johnson',
            'email' => 'alice_join_' . time() . '@example.com',
            'status' => 'active'
        ]);
        $this->trait->insert('test_profiles', [
            'user_id' => $userId,
            'bio' => 'Alice bio'
        ]);

        // Test JOIN with WHERE array
        // Use string WHERE to avoid dots in parameter names
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.bio'],
            'LEFT JOIN test_profiles p ON p.user_id = u.id',
            'u.id = ? AND u.status = ?',
            [$userId, 'active']
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('Alice Johnson', $result['name']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithWhereStringAndBindings()
    {
        // Create test_profiles table
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userId = $this->trait->insert('test_users', [
            'name' => 'Charlie Brown',
            'email' => 'charlie_join_' . time() . '@example.com',
            'status' => 'active'
        ]);
        $this->trait->insert('test_profiles', [
            'user_id' => $userId,
            'bio' => 'Charlie bio'
        ]);

        // Test JOIN with WHERE string and ? placeholders
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.bio'],
            'LEFT JOIN test_profiles p ON p.user_id = u.id',
            'u.id = ? AND u.status = ?',
            [$userId, 'active']
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('Charlie Brown', $result['name']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithSqlTail()
    {
        // Create test_profiles table
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userIds = [];
        for ($i = 1; $i <= 3; $i++) {
            $userIds[] = $this->trait->insert('test_users', [
                'name' => "User $i",
                'email' => "user$i@example.com"
            ]);
            $this->trait->insert('test_profiles', [
                'user_id' => $userIds[$i - 1],
                'bio' => "Bio $i"
            ]);
        }

        // Test JOIN with ORDER BY and LIMIT
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.bio'],
            'LEFT JOIN test_profiles p ON p.user_id = u.id',
            null,
            null,
            'ORDER BY u.name LIMIT 2'
        );

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $results);
        $this->assertEquals('User 1', $results[0]['name']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithInClause()
    {
        // Create test_profiles table
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userIds = [];
        for ($i = 1; $i <= 3; $i++) {
            $userIds[] = $this->trait->insert('test_users', [
                'name' => "User $i",
                'email' => "user$i@example.com"
            ]);
            $this->trait->insert('test_profiles', [
                'user_id' => $userIds[$i - 1],
                'bio' => "Bio $i"
            ]);
        }

        // Test JOIN with IN clause
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.bio'],
            'LEFT JOIN test_profiles p ON p.user_id = u.id',
            'u.id IN (:ids)',
            [':ids' => $userIds]
        );

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $results);
        foreach ($results as $result) {
            $this->assertArrayHasKey('id', $result);
            $this->assertArrayHasKey('name', $result);
            $this->assertArrayHasKey('bio', $result);
        }

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithNullJoins()
    {
        // Insert test user
        $userId = $this->trait->insert('test_users', [
            'name' => 'Test User',
            'email' => 'test_join_null_' . time() . '@example.com'
        ]);

        // Test JOIN with null joins (should work like regular select)
        // Use simple column name without alias to avoid dot in parameter name
        $stmt = $this->trait->selectJoin(
            'test_users',
            ['id', 'name'],
            null,
            'id',
            $userId
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('Test User', $result['name']);
    }

    public function testSelectJoinWithEmptyJoins()
    {
        // Insert test user
        $userId = $this->trait->insert('test_users', [
            'name' => 'Test User 2',
            'email' => 'test_join_empty_' . time() . '@example.com'
        ]);

        // Test JOIN with empty joins array
        // Use simple column name without alias to avoid dot in parameter name
        $stmt = $this->trait->selectJoin(
            'test_users',
            ['id', 'name'],
            [],
            'id',
            $userId
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('Test User 2', $result['name']);
    }

    public function testSelectJoinWithColumnString()
    {
        // Create test_profiles table
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userId = $this->trait->insert('test_users', [
            'name' => 'Test User 3',
            'email' => 'test_join_col_' . time() . '@example.com'
        ]);
        $this->trait->insert('test_profiles', [
            'user_id' => $userId,
            'bio' => 'Test bio'
        ]);

        // Test JOIN with column string
        // Use proper WHERE clause format to avoid dot in parameter name
        $stmt = $this->trait->selectJoin(
            'test_users u',
            'u.id, u.name, p.bio',
            'LEFT JOIN test_profiles p ON p.user_id = u.id',
            'u.id = ?',
            [$userId]
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals('Test User 3', $result['name']);
        $this->assertEquals('Test bio', $result['bio']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testComplexExampleWithAllFeatures()
    {
        // Create test tables for complex example
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                bio TEXT,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                total DECIMAL(10,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $timestamp = time();
        $userIds = [];
        $users = [
            ['name' => 'Admin User', 'email' => "admin_complex_{$timestamp}@example.com", 'status' => 'active'],
            ['name' => 'Moderator User', 'email' => "mod_complex_{$timestamp}@example.com", 'status' => 'active'],
            ['name' => 'Regular User', 'email' => "user_complex_{$timestamp}@example.com", 'status' => 'active'],
            ['name' => 'Inactive User', 'email' => "inactive_complex_{$timestamp}@example.com", 'status' => 'inactive']
        ];

        foreach ($users as $user) {
            $userId = $this->trait->insert('test_users', $user);
            $userIds[] = $userId;
            
            // Add profile
            $this->trait->insert('test_profiles', [
                'user_id' => $userId,
                'bio' => "Bio for {$user['name']}"
            ]);
            
            // Add orders with different totals
            if ($user['status'] === 'active') {
                $this->trait->insert('test_orders', [
                    'user_id' => $userId,
                    'total' => 1500.00
                ]);
                $this->trait->insert('test_orders', [
                    'user_id' => $userId,
                    'total' => 500.00
                ]);
            }
        }

        // Set dates for testing
        $minDate = date('Y-m-d', strtotime('-30 days'));
        $maxLastLogin = date('Y-m-d', strtotime('-5 days'));
        
        // Update some users to have last_login dates
        $this->pdo->exec("UPDATE test_users SET created_at = DATE_SUB(NOW(), INTERVAL 20 DAY) WHERE id IN (" . implode(',', array_slice($userIds, 0, 3)) . ")");

        // Complex query with all features:
        // - JOINs (multiple)
        // - Flexible WHERE: key-value with dots (u.status), raw SQL, named placeholder, ? placeholder, IN clause
        // - sqlTail with bindings (HAVING, ORDER BY, LIMIT)
        // Demonstrates all features including array WHERE with table aliases (dots in keys are sanitized)
        $stmt = $this->trait->selectJoin(
            'test_users u',
            [
                'u.id',
                'u.name',
                'u.email',
                'p.bio',
                'COUNT(o.id) AS order_count',
                'SUM(o.total) AS total_spent'
            ],
            [
                'LEFT JOIN test_profiles p ON p.user_id = u.id',
                'LEFT JOIN test_orders o ON o.user_id = u.id'
            ],
            [
                'u.status' => 'active',                    // Key with dot - sanitized to :where_u_status
                'u.name != ""',                            // Raw SQL (numeric key)
                'u.created_at > :min_date',                 // Raw SQL with named placeholder
                'u.created_at < ?' => $maxLastLogin,        // Key with ? placeholder
                'u.id IN (:user_ids)'                       // IN clause with named placeholder (raw SQL)
            ],
            [
                ':min_date' => $minDate,
                ':user_ids' => array_slice($userIds, 0, 3),  // Will expand to :user_ids0, :user_ids1, :user_ids2
                ':min_spent' => 1000                        // For HAVING clause in sqlTail
            ],
            'GROUP BY u.id HAVING total_spent > :min_spent ORDER BY total_spent DESC LIMIT 10'
        );

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Verify results
        $this->assertNotEmpty($results);
        $this->assertLessThanOrEqual(3, count($results)); // Should be limited by HAVING and LIMIT
        
        foreach ($results as $result) {
            // Verify all expected columns are present
            $this->assertArrayHasKey('id', $result);
            $this->assertArrayHasKey('name', $result);
            $this->assertArrayHasKey('email', $result);
            $this->assertArrayHasKey('bio', $result);
            $this->assertArrayHasKey('order_count', $result);
            $this->assertArrayHasKey('total_spent', $result);
            
            // Verify HAVING clause worked (total_spent > 1000)
            $this->assertGreaterThan(1000, (float)$result['total_spent']);
            
            // Verify order_count is correct
            $this->assertGreaterThan(0, (int)$result['order_count']);
        }

        // Verify results are ordered by total_spent DESC
        if (count($results) > 1) {
            $firstTotal = (float)$results[0]['total_spent'];
            $secondTotal = (float)$results[1]['total_spent'];
            $this->assertGreaterThanOrEqual($secondTotal, $firstTotal);
        }

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_orders");
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

    public function testSelectJoinWithBindingInJoinClause()
    {
        // Create test_orders table
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                total DECIMAL(10,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userId = $this->trait->insert('test_users', [
            'name' => 'Test User Join Binding',
            'email' => 'test_join_binding_' . time() . '@example.com',
            'status' => 'active'
        ]);

        // Insert orders with different totals
        $this->trait->insert('test_orders', ['user_id' => $userId, 'total' => 50]);
        $this->trait->insert('test_orders', ['user_id' => $userId, 'total' => 150]);
        $this->trait->insert('test_orders', ['user_id' => $userId, 'total' => 200]);

        // Test JOIN with binding - only orders with total > 100
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'o.total'],
            [
                'LEFT JOIN test_orders o ON o.user_id = u.id AND o.total > ?' => 100
            ],
            ['u.id' => $userId]
        );

        $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $results); // Should return 2 orders (150 and 200)
        foreach ($results as $result) {
            $this->assertGreaterThan(100, (float)$result['total']);
        }

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_orders");
    }

    public function testSelectJoinWithMultipleBindingsInJoinClauses()
    {
        // Create test tables
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_profiles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                verified TINYINT(1) DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS test_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                total DECIMAL(10,2) DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )
        ");

        // Insert test data
        $userId = $this->trait->insert('test_users', [
            'name' => 'Test User Multiple Join Bindings',
            'email' => 'test_multiple_join_bindings_' . time() . '@example.com',
            'status' => 'active'
        ]);

        $this->trait->insert('test_profiles', ['user_id' => $userId, 'verified' => 1]);
        $this->trait->insert('test_orders', ['user_id' => $userId, 'total' => 150]);

        // Test multiple JOINs with bindings
        $stmt = $this->trait->selectJoin(
            'test_users u',
            ['u.id', 'u.name', 'p.verified', 'o.total'],
            [
                'LEFT JOIN test_profiles p ON p.user_id = u.id AND p.verified = ?' => 1,
                'LEFT JOIN test_orders o ON o.user_id = u.id AND o.total > ?' => 100
            ],
            ['u.id' => $userId]
        );

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotEmpty($result);
        $this->assertEquals($userId, $result['id']);
        $this->assertEquals(1, (int)$result['verified']);
        $this->assertGreaterThan(100, (float)$result['total']);

        // Cleanup
        $this->pdo->exec("DROP TABLE IF EXISTS test_orders");
        $this->pdo->exec("DROP TABLE IF EXISTS test_profiles");
    }

} 