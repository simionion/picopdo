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
                insert as public;
                select as public;
                selectOne as public;
                update as public;
                delete as public;
                exists as public;
                selectAll as public;
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
        // Clean up test table
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
} 