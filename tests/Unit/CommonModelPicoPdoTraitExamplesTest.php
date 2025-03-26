<?php

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PHPUnit\Framework\TestCase;
use PDO;
use PDOStatement;

class CommonModelPicoPdoTraitExamplesTest extends TestCase
{
    use CommonModelPicoPdoTrait;

    protected PDO $pdo;
    private PDOStatement $statement;

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo = $this->createMock(PDO::class);
        $this->statement = $this->createMock(PDOStatement::class);
    }

    public function testBasicCrudOperations()
    {
        // Mock insert
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('INSERT INTO users (`name`,`email`,`status`) VALUES (?,?,?)')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                'John Doe',
                'john@example.com',
                'active'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);

        $this->pdo->expects($this->once())
            ->method('lastInsertId')
            ->willReturn('1');

        $id = $this->insert('users', [
            'name' => 'John Doe',
            'email' => 'john@example.com',
            'status' => 'active'
        ]);

        $this->assertEquals('1', $id);
    }

    public function testSelectWithMultipleConditions()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM `users` WHERE `id` = :where_id AND `status` = :where_status LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_id' => 1,
                ':where_status' => 'active'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                'name' => 'John Doe',
                'email' => 'john@example.com'
            ]);

        $user = $this->select('users', ['name', 'email'], [
            'id' => 1,
            'status' => 'active'
        ]);

        $this->assertEquals([
            'name' => 'John Doe',
            'email' => 'john@example.com'
        ], $user);
    }

    public function testSelectAllWithInClause()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM `users` WHERE status = :where_0 AND role IN (:where_10,:where_11)')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_0' => 'active',
                ':where_10' => 'admin',
                ':where_11' => 'moderator'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['name' => 'Admin User', 'email' => 'admin@example.com'],
                ['name' => 'Mod User', 'email' => 'mod@example.com']
            ]);

        $users = $this->selectAll('users', ['name', 'email'],
            'status = ? AND role IN (?)',
            ['active', ['admin', 'moderator']]
        );

        $this->assertEquals([
            ['name' => 'Admin User', 'email' => 'admin@example.com'],
            ['name' => 'Mod User', 'email' => 'mod@example.com']
        ], $users);
    }

    public function testUpdateWithMultipleConditions()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('UPDATE `users` SET `status` = :set_status WHERE role IN (:where_00,:where_01) AND created_at < NOW()')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':set_status' => 'inactive',
                ':where_00' => 'admin',
                ':where_01' => 'moderator'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('rowCount')
            ->willReturn(2);

        $result = $this->update('users', 
            ['status' => 'inactive'],
            'role IN (?) AND created_at < NOW()',
            [['admin', 'moderator']]
        );

        $this->assertEquals(2, $result);
    }

    public function testDeleteWithRawCondition()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('DELETE FROM `users` WHERE status = :where_0 AND last_login < DATE_SUB(NOW(), INTERVAL :where_1 DAY)')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_0' => 'inactive',
                ':where_1' => 30
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);

        $result = $this->delete('users',
            'status = ? AND last_login < DATE_SUB(NOW(), INTERVAL ? DAY)',
            ['inactive', 30]
        );

        $this->assertEquals(1, $result);
    }

    public function testExistsWithMultipleConditions()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT 1 as `true` FROM `users` WHERE `email` = :where_email AND `status` = :where_status LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_email' => 'john@example.com',
                ':where_status' => 'active'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);

        $exists = $this->exists('users', [
            'email' => 'john@example.com',
            'status' => 'active'
        ]);

        $this->assertTrue($exists);
    }

    public function testComplexWhereConditions()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM `users` WHERE status = :where_0 AND role IN (:where_10,:where_11) AND created_at < NOW() AND email LIKE :where_2 AND age > :where_3')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_0' => 'active',
                ':where_10' => 'admin',
                ':where_11' => 'moderator',
                ':where_2' => '%@example.com',
                ':where_3' => 18
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['name' => 'John Doe', 'email' => 'john@example.com']
            ]);

        $users = $this->selectAll('users', ['name', 'email'],
            'status = ? AND role IN (?) AND created_at < NOW() AND email LIKE ? AND age > ?',
            ['active', ['admin', 'moderator'], '%@example.com', 18]
        );

        $this->assertEquals([
            ['name' => 'John Doe', 'email' => 'john@example.com']
        ], $users);
    }

    public function testSelectWithColumnAliases()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name AS full_name, COUNT(*) AS total_count FROM `users` WHERE `id` = :where_id LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_id' => 1])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                'full_name' => 'John Doe',
                'total_count' => 1
            ]);

        $user = $this->select('users', ['name AS full_name', 'COUNT(*) AS total_count'], 'id', 1);

        $this->assertEquals([
            'full_name' => 'John Doe',
            'total_count' => 1
        ], $user);
    }

    public function testSelectAllWithColumnAliases()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name AS user_name, COUNT(*) AS order_count FROM `users` WHERE status = :status')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':status' => 'active'])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                ['user_name' => 'John Doe', 'order_count' => 5],
                ['user_name' => 'Jane Smith', 'order_count' => 3]
            ]);

        $users = $this->selectAll('users', [
            'name AS user_name',
            'COUNT(*) AS order_count'
        ], 'status = :status', [':status' => 'active']);

        $this->assertEquals([
            ['user_name' => 'John Doe', 'order_count' => 5],
            ['user_name' => 'Jane Smith', 'order_count' => 3]
        ], $users);
    }

    public function testSelectWithComplexColumnExpressions()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT CONCAT(first_name, " ", last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) AS age FROM `users` WHERE `id` = :where_id LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_id' => 1])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                'full_name' => 'John Doe',
                'age' => 30
            ]);

        $user = $this->select('users', [
            'CONCAT(first_name, " ", last_name) AS full_name',
            'TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) AS age'
        ], 'id', 1);

        $this->assertEquals([
            'full_name' => 'John Doe',
            'age' => 30
        ], $user);
    }

    public function testSelectWithScalarValueBinding()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM `users` WHERE user = ? LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([22])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                'name' => 'John Doe',
                'email' => 'john@example.com'
            ]);

        $user = $this->select('users', ['name', 'email'], 'user = ?', 22);

        $this->assertEquals([
            'name' => 'John Doe',
            'email' => 'john@example.com'
        ], $user);
    }

    public function testSelectWithTooFewParameters()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM `users` WHERE status = :where_0 AND role = :where_1 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 'active'])
            ->willThrowException(new \PDOException('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens'));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens');

        $this->select('users', ['name', 'email'], 'status = ? AND role = ?', ['active']);
    }

    public function testSelectWithTooManyParameters()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM `users` WHERE status = :where_0 LIMIT 1')
            ->willReturn($this->statement);

        // We expect execute to be called with both parameters, as PHP maintains both named and numeric indices
        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_0' => 'active',
                1 => 'admin'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John', 'email' => 'john@example.com']);

        // This should work, using both parameters
        $result = $this->select('users', ['name', 'email'], 'status = ?', ['active', 'admin']);
        
        $this->assertEquals(['name' => 'John', 'email' => 'john@example.com'], $result);
    }

    public function testSelectWithNamedParametersMismatch()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name, email FROM `users` WHERE status = :status AND role = :role LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':status' => 'active'])
            ->willThrowException(new \PDOException('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens'));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens');

        $this->select('users', ['name', 'email'], 'status = :status AND role = :role', [':status' => 'active']);
    }

    public function testSelectWithArrayParameter()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users` WHERE id IN (:ids0,:ids1,:ids2) LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':ids0' => 1,
                ':ids1' => 2,
                ':ids2' => 3
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe']);

        $user = $this->select('users', ['name'], 'id IN (:ids)', [':ids' => [1, 2, 3]]);

        $this->assertEquals(['name' => 'John Doe'], $user);
    }

    public function testSelectWithEmptyArrayParameter()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users` WHERE id IN (:ids) LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':ids' => []])
            ->willThrowException(new \PDOException('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens'));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens');

        $this->select('users', ['name'], 'id IN (:ids)', [':ids' => []]);
    }

    public function testSelectWithNullParameters()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users` WHERE status = :where_0 AND deleted_at IS :where_1 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 'active', ':where_1' => null])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['name' => 'John Doe']);

        $user = $this->select('users', ['name'], 'status = ? AND deleted_at IS ?', ['active', null]);

        $this->assertEquals(['name' => 'John Doe'], $user);
    }

    public function testSelectWithMixedParameterTypes()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users` WHERE status = :status AND role = ? LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':status' => 'active', 'admin'])
            ->willThrowException(new \PDOException('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens'));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens');

        $this->select('users', ['name'], 'status = :status AND role = ?', [':status' => 'active', 'admin']);
    }

    public function testSelectWithSqlInjectionAttempt()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users` WHERE id = :where_0 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => "1' OR '1'='1"])
            ->willThrowException(new \PDOException('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens'));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens');

        $this->select('users', ['name'], 'id = ?', ["1' OR '1'='1"]);
    }

    public function testSelectWithSpecialCharactersInColumnNames()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT user-name AS display_name, email@domain AS contact FROM `users` WHERE id = :where_0 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 1])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['display_name' => 'John Doe', 'contact' => 'john@example.com']);

        $user = $this->select('users', ['user-name AS display_name', 'email@domain AS contact'], 'id = ?', [1]);

        $this->assertEquals(['display_name' => 'John Doe', 'contact' => 'john@example.com'], $user);
    }

    public function testSelectWithEmptyWhereClause()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users`  LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([])
            ->willThrowException(new \PDOException('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens'));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[HY093]: Invalid parameter number: number of bound variables does not match number of tokens');

        $this->select('users', ['name'], '', []);
    }

    public function testSelectWithInvalidColumnName()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT invalid_column FROM `users` WHERE id = :where_0 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 1])
            ->willThrowException(new \PDOException('SQLSTATE[42S22]: Column not found: 1054 Unknown column \'invalid_column\' in \'field list\''));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[42S22]: Column not found: 1054 Unknown column \'invalid_column\' in \'field list\'');

        $this->select('users', ['invalid_column'], 'id = ?', [1]);
    }

    public function testSelectWithTableNameInjection()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users; DROP TABLE users; --` WHERE id = :where_0 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 1])
            ->willThrowException(new \PDOException('SQLSTATE[42S02]: Base table or view not found: 1146 Table \'test.users; DROP TABLE users; --\' doesn\'t exist'));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[42S02]: Base table or view not found: 1146 Table \'test.users; DROP TABLE users; --\' doesn\'t exist');

        $this->select('users; DROP TABLE users; --', ['name'], 'id = ?', [1]);
    }

    public function testSelectWithParameterTypeMismatch()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT name FROM `users` WHERE age > :where_0 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 'not_a_number'])
            ->willThrowException(new \PDOException('SQLSTATE[22007]: Invalid datetime format: 1292 Truncated incorrect DOUBLE value: \'not_a_number\''));

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('SQLSTATE[22007]: Invalid datetime format: 1292 Truncated incorrect DOUBLE value: \'not_a_number\'');

        $this->select('users', ['name'], 'age > ?', ['not_a_number']);
    }

    public function testSelectWithComplexColumnNames()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT user-name AS display_name, email@domain AS contact FROM `users` WHERE id = :where_0 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 1])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn(['display_name' => 'John Doe', 'contact' => 'john@example.com']);

        $user = $this->select('users', ['user-name AS display_name', 'email@domain AS contact'], 'id = ?', [1]);

        $this->assertEquals(['display_name' => 'John Doe', 'contact' => 'john@example.com'], $user);
    }

    public function testSelectWithNamedPlaceholdersInColumnsAndPositionalInWhere()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT CONCAT(first_name, " ", last_name) AS full_name, TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age FROM `users` WHERE status = :where_0 AND role = :where_1 LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':report_date' => '2024-01-01',  // Named placeholder from SELECT
                ':where_0' => 'active',          // Positional from WHERE
                ':where_1' => 'admin'            // Positional from WHERE
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                'full_name' => 'John Doe',
                'age' => 30
            ]);

        // Note: bindings array contains both named (:report_date) and indexed values for WHERE clause
        $user = $this->select('users', 
            [
                'CONCAT(first_name, " ", last_name) AS full_name', 
                'TIMESTAMPDIFF(YEAR, birth_date, :report_date) AS age'
            ],
            'status = ? AND role = ?',
            [':report_date' => '2024-01-01', 'active', 'admin']  // Mixed named and indexed bindings
        );

        $this->assertEquals([
            'full_name' => 'John Doe',
            'age' => 30
        ], $user);
    }

    public function testSelectWithJoinAndAggregates()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT u.name, u.email, COUNT(o.id) as order_count FROM `users u` LEFT JOIN orders o ON u.id = o.user_id WHERE u.status = :where_0 GROUP BY u.id LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 'active'])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                'name' => 'John Doe',
                'email' => 'john@example.com',
                'order_count' => 5
            ]);

        $user = $this->select('users u', 
            ['u.name', 'u.email', 'COUNT(o.id) as order_count'],
            'LEFT JOIN orders o ON u.id = o.user_id WHERE u.status = ? GROUP BY u.id',
            ['active']
        );

        $this->assertEquals([
            'name' => 'John Doe',
            'email' => 'john@example.com',
            'order_count' => 5
        ], $user);
    }

    public function testSelectAllWithMultipleJoinsAndSubquery()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT u.name, u.email, o.total FROM `users u` INNER JOIN orders o ON u.id = o.user_id WHERE u.status = :where_0 ORDER BY o.total DESC')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([':where_0' => 'active'])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_ASSOC)
            ->willReturn([
                [
                    'name' => 'John Doe',
                    'email' => 'john@example.com',
                    'total' => 1500.00
                ],
                [
                    'name' => 'Jane Smith',
                    'email' => 'jane@example.com',
                    'total' => 1200.00
                ]
            ]);

        $users = $this->selectAll('users u', 
            ['u.name', 'u.email', 'o.total'],
            'INNER JOIN orders o ON u.id = o.user_id WHERE u.status = ? ORDER BY o.total DESC',
            ['active']
        );

        $this->assertEquals([
            [
                'name' => 'John Doe',
                'email' => 'john@example.com',
                'total' => 1500.00
            ],
            [
                'name' => 'Jane Smith',
                'email' => 'jane@example.com',
                'total' => 1200.00
            ]
        ], $users);
    }

    public function testExistsWithJoinAndSubquery()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('SELECT 1 as `true` FROM `users u` INNER JOIN orders o ON u.id = o.user_id WHERE u.email = :where_email AND o.status = :where_status LIMIT 1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_email' => 'john@example.com',
                ':where_status' => 'completed'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('rowCount')
            ->willReturn(1);

        $exists = $this->exists('users u', 
            'INNER JOIN orders o ON u.id = o.user_id WHERE u.email = :where_email AND o.status = :where_status',
            [':where_email' => 'john@example.com', ':where_status' => 'completed']
        );

        $this->assertTrue($exists);
    }

    public function testUpdateWithJoinAndSubquery()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('UPDATE `users u` SET `status` = :set_status INNER JOIN orders o ON u.id = o.user_id WHERE o.status = :where_0')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':set_status' => 'inactive',
                ':where_0' => 'completed'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('rowCount')
            ->willReturn(2);

        $result = $this->update('users u',
            ['status' => 'inactive'],
            'INNER JOIN orders o ON u.id = o.user_id WHERE o.status = ?',
            ['completed']
        );

        $this->assertEquals(2, $result);
    }

    public function testDeleteWithJoinAndSubquery()
    {
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with('DELETE FROM `users u` INNER JOIN orders o ON u.id = o.user_id WHERE u.status = :where_0 AND o.status = :where_1')
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->with([
                ':where_0' => 'inactive',
                ':where_1' => 'cancelled'
            ])
            ->willReturn(true);

        $this->statement->expects($this->once())
            ->method('rowCount')
            ->willReturn(5);

        $result = $this->delete('users u',
            'INNER JOIN orders o ON u.id = o.user_id WHERE u.status = ? AND o.status = ?',
            ['inactive', 'cancelled']
        );

        $this->assertEquals(5, $result);
    }
} 