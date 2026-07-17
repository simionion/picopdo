<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests\Integration;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;

/**
 * One test per documented code example in CommonModelPicoPdoTrait PHPDoc and README.
 * Table names use `doc_users` / `doc_profiles` instead of `users` / `profiles` from the docs.
 */
class CommonModelPicoPdoTraitDocExamplesTest extends TestCase
{
    private const TABLE_USERS = 'doc_users';

    private const TABLE_PROFILES = 'doc_profiles';

    private object $db;

    private PDO $pdo;

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
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                status ENUM(\'active\', \'pending\', \'inactive\', \'archived\') DEFAULT \'active\',
                email_verified INT DEFAULT 0,
                last_login DATETIME NULL,
                date_verified DATETIME NULL,
                views INT DEFAULT 0,
                role VARCHAR(64) NULL,
                is_active TINYINT DEFAULT 1,
                uuid CHAR(36) NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )'
        );
        $this->pdo->exec(
            'CREATE TABLE ' . self::TABLE_PROFILES . ' (
                user_id INT PRIMARY KEY,
                bio TEXT,
                status VARCHAR(64) DEFAULT \'active\'
            )'
        );

        $this->db = new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                prepExec as public;
                exists as public;
                insert as public;
                insertReplace as public;
                insertIgnore as public;
                insertOnDuplicateKeyUpdate as public;
                update as public;
                select as public;
                selectOne as public;
                selectAll as public;
                selectCompose as public;
                delete as public;
            }

            public function __construct(PDO $pdo)
            {
                $this->pdo = $pdo;
            }
        };
    }

    protected function tearDown(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE_PROFILES);
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE_USERS);
        parent::tearDown();
    }

    private function seedUser(
        string $name,
        string $email,
        array $extra = []
    ): int {
        $row = array_merge([
            'name' => $name,
            'email' => $email,
            'status' => 'active',
        ], $extra);

        return (int) $this->db->insert(self::TABLE_USERS, $row);
    }

    // ——— prepExec() doc examples ———

    public function testDocPrepExecSimplePositionalPlaceholder(): void
    {
        $id = $this->seedUser('Prep', 'prep-pos@example.com');
        $row = $this->db->prepExec(
            'SELECT * FROM ' . self::TABLE_USERS . ' WHERE id = ?',
            [$id]
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Prep', $row['name']);
    }

    public function testDocPrepExecNamedPlaceholders(): void
    {
        $id = $this->seedUser('Prep', 'prep-named@example.com');
        $row = $this->db->prepExec(
            'SELECT * FROM ' . self::TABLE_USERS . ' WHERE id = :id',
            ['id' => $id]
        )->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('Prep', $row['name']);
    }

    public function testDocPrepExecNamedInExpansion(): void
    {
        $ids = [
            $this->seedUser('A', 'prep-in-a@example.com'),
            $this->seedUser('B', 'prep-in-b@example.com'),
            $this->seedUser('C', 'prep-in-c@example.com'),
        ];
        $stmt = $this->db->prepExec(
            'SELECT id FROM ' . self::TABLE_USERS . ' WHERE id IN (:ids)',
            [':ids' => $ids]
        );
        $this->assertCount(3, $stmt->fetchAll(PDO::FETCH_COLUMN));
    }

    // ——— Common arguments: JOIN in $table ———

    public function testDocTableSelectAllWithSimpleJoin(): void
    {
        $userId = $this->seedUser('Join User', 'join@example.com', ['status' => 'active']);
        $this->pdo->exec(
            'INSERT INTO ' . self::TABLE_PROFILES . ' (user_id, bio, status) VALUES ('
            . $userId . ", 'Bio text', 'active')"
        );

        $rows = $this->db->selectAll(
            self::TABLE_USERS . ' u INNER JOIN ' . self::TABLE_PROFILES . ' p ON p.user_id = u.id',
            ['u.name', 'p.bio'],
            ['u.status' => 'active']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Join User', $rows[0]['name']);
        $this->assertSame('Bio text', $rows[0]['bio']);
    }

    // ——— exists() doc examples ———

    public function testDocExistsClassicKeyValue(): void
    {
        $id = $this->seedUser('Exists', 'exists@example.com');
        $this->assertTrue($this->db->exists(self::TABLE_USERS, 'id', $id));
    }

    public function testDocExistsAssociativeArray(): void
    {
        $this->seedUser('Exists', 'exists-assoc@example.com', ['email_verified' => 1]);
        $this->assertTrue($this->db->exists(self::TABLE_USERS, [
            'status' => 'active',
            'email_verified' => 1,
        ]));
    }

    public function testDocExistsAssociativeArrayWithRawSql(): void
    {
        $status = 'active';
        $date = '2024-01-01 00:00:00';
        $this->seedUser('Exists', 'exists-raw@example.com', [
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);

        $this->assertTrue($this->db->exists(self::TABLE_USERS, [
            'status' => $status,
            'email_verified != 0',
            'created_at > :date',
        ], [':date' => $date]));
    }

    public function testDocExistsCustomWhereClause(): void
    {
        $this->seedUser('Exists', 'user@example.com', ['created_at' => '2024-06-01 00:00:00']);
        $this->assertTrue($this->db->exists(
            self::TABLE_USERS,
            'email = ? AND created_at > ?',
            ['user@example.com', '2024-01-01']
        ));
    }

    public function testDocExistsMixedNamedAndQuestionMarkPlaceholders(): void
    {
        $id = $this->seedUser('Exists', 'exists-mixed@example.com');
        $this->assertTrue($this->db->exists(
            self::TABLE_USERS,
            'status = :status AND id > ?',
            [':status' => 'active', $id - 1]
        ));
    }

    public function testDocExistsInNamedPlaceholder(): void
    {
        $ids = [
            $this->seedUser('E1', 'e1@example.com'),
            $this->seedUser('E2', 'e2@example.com'),
        ];
        $this->assertTrue($this->db->exists(self::TABLE_USERS, 'id IN (:ids)', [':ids' => $ids]));
    }

    public function testDocExistsInQuestionMarkPlaceholder(): void
    {
        $ids = [
            $this->seedUser('E3', 'e3@example.com'),
            $this->seedUser('E4', 'e4@example.com'),
        ];
        $this->assertTrue($this->db->exists(self::TABLE_USERS, 'id IN (?)', [$ids]));
    }

    public function testDocExistsAdvancedAssociativeBindings(): void
    {
        $status = 'active';
        $date = '2024-01-01 00:00:00';
        $this->seedUser('Advanced', 'advanced-exists@example.com', [
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);

        $this->assertTrue($this->db->exists(self::TABLE_USERS, [
            'status' => $status,
            'email_verified != 0',
            'created_at > :date',
            'created_at > ?' => $date,
        ], [':date' => $date]));
    }

    // ——— insert() doc examples ———

    public function testDocInsertBasic(): void
    {
        $id = $this->db->insert(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john@example.com',
        ]);
        $this->assertGreaterThan(0, $id);
        $this->assertSame('John', $this->pdo->query(
            'SELECT name FROM ' . self::TABLE_USERS . ' WHERE id = ' . (int) $id
        )->fetchColumn());
    }

    public function testDocInsertWithRawSql(): void
    {
        $id = $this->db->insert(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-raw@example.com',
            'created_at = NOW()',
            'uuid = UUID()',
        ]);
        $row = $this->pdo->query('SELECT created_at FROM ' . self::TABLE_USERS . ' WHERE id = ' . (int) $id)
            ->fetch(PDO::FETCH_ASSOC);
        $this->assertNotNull($row['created_at']);
    }

    public function testDocInsertReplaceViaOptions(): void
    {
        $id = $this->db->insert(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-replace@example.com',
        ], ['mode' => 'REPLACE']);
        $this->assertGreaterThan(0, $id);
    }

    public function testDocInsertIgnoreViaOptions(): void
    {
        $this->db->insert(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-ignore@example.com',
        ]);
        $result = $this->db->insert(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-ignore@example.com',
        ], ['mode' => 'INSERT IGNORE', 'meta' => true]);
        $this->assertSame('noop', $result['status']);
    }

    public function testDocInsertOnDuplicateKeyUpdateViaOptions(): void
    {
        $this->db->insert(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-upsert@example.com',
        ]);
        $result = $this->db->insert(self::TABLE_USERS, [
            'name' => 'John Updated',
            'email' => 'john-upsert@example.com',
        ], [
            'onDuplicateKeyUpdate' => ['name' => 'John Updated', 'email' => 'john-upsert@example.com'],
            'meta' => true,
        ]);
        $this->assertContains($result['status'], ['updated', 'inserted']);
    }

    public function testDocInsertWithMeta(): void
    {
        $result = $this->db->insert(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-meta@example.com',
        ], ['meta' => true]);

        $this->assertSame('inserted', $result['status']);
        $this->assertSame(1, $result['rows']);
        $this->assertGreaterThan(0, $result['id']);
    }

    public function testDocInsertMultipleRows(): void
    {
        $result = $this->db->insert(self::TABLE_USERS, [
            ['name' => 'Ion', 'email' => 'ion@example.com', 'created_at = NOW()', 'is_active' => 1],
            ['name' => 'Ani', 'email' => 'ani@example.com', 'created_at = NOW()', 'is_active' => 0],
        ]);

        $this->assertSame(2, $result['rows']);
        $this->assertSame(1, (int) $this->pdo->query(
            'SELECT is_active FROM ' . self::TABLE_USERS . " WHERE name = 'Ion'"
        )->fetchColumn());
        $this->assertSame(0, (int) $this->pdo->query(
            'SELECT is_active FROM ' . self::TABLE_USERS . " WHERE name = 'Ani'"
        )->fetchColumn());
    }

    public function testDocInsertIgnoreWrapper(): void
    {
        $first = $this->db->insertIgnore(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-wrapper-ignore@example.com',
        ]);
        $this->assertSame('inserted', $first['status']);

        $second = $this->db->insertIgnore(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-wrapper-ignore@example.com',
        ]);
        $this->assertSame('noop', $second['status']);
        $this->assertSame(0, $second['rows']);
    }

    public function testDocInsertOnDuplicateKeyUpdateWrapper(): void
    {
        $first = $this->db->insertOnDuplicateKeyUpdate(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-wrapper-upsert@example.com',
        ], ['name' => 'John', 'email' => 'john-wrapper-upsert@example.com']);
        $this->assertSame('inserted', $first['status']);

        $second = $this->db->insertOnDuplicateKeyUpdate(self::TABLE_USERS, [
            'name' => 'John',
            'email' => 'john-wrapper-upsert@example.com',
        ], ['name' => 'John', 'email' => 'john-wrapper-upsert@example.com']);
        $this->assertContains($second['status'], ['updated', 'noop']);
    }

    // ——— update() doc examples ———

    public function testDocUpdateClassicKeyValue(): void
    {
        $id = $this->seedUser('Before', 'upd-classic@example.com');
        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], 'id', $id);
        $this->assertSame(1, $rows);
        $this->assertSame('John', $this->pdo->query(
            'SELECT name FROM ' . self::TABLE_USERS . ' WHERE id = ' . $id
        )->fetchColumn());
    }

    public function testDocUpdateRawSqlInSet(): void
    {
        $id = $this->seedUser('Before', 'upd-raw@example.com');
        $this->db->update(self::TABLE_USERS, [
            'name' => 'John',
            'date_verified = NOW()',
        ], 'id', $id);
        $this->assertNotNull($this->pdo->query(
            'SELECT date_verified FROM ' . self::TABLE_USERS . ' WHERE id = ' . $id
        )->fetchColumn());
    }

    public function testDocUpdateAssociativeWhere(): void
    {
        $id = $this->seedUser('Before', 'upd-assoc@example.com', ['email_verified' => 1]);
        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], [
            'id' => $id,
            'email_verified' => 1,
        ]);
        $this->assertSame(1, $rows);
    }

    public function testDocUpdateAssociativeWhereWithRawSql(): void
    {
        $date = '2024-01-01 00:00:00';
        $id = $this->seedUser('Before', 'upd-assoc-raw@example.com', [
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);
        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], [
            'id' => $id,
            'email_verified != 0',
            'created_at > :date',
        ], [':date' => $date]);
        $this->assertSame(1, $rows);
    }

    public function testDocUpdateCustomWhereOr(): void
    {
        $id = $this->seedUser('Before', 'john@example.com', ['status' => 'active']);
        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], 'email = ? OR status = ?', [
            'john@example.com',
            'active',
        ]);
        $this->assertSame(1, $rows);
    }

    public function testDocUpdateMixedPlaceholders(): void
    {
        $id = $this->seedUser('Before', 'upd-mixed@example.com');
        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], 'status = :status AND id > ?', [
            ':status' => 'active',
            $id - 1,
        ]);
        $this->assertSame(1, $rows);
    }

    public function testDocUpdateInNamedPlaceholder(): void
    {
        $ids = [
            $this->seedUser('U1', 'upd-in-1@example.com'),
            $this->seedUser('U2', 'upd-in-2@example.com'),
        ];
        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], 'id IN (:ids)', [':ids' => $ids]);
        $this->assertSame(2, $rows);
    }

    public function testDocUpdateInQuestionMarkPlaceholder(): void
    {
        $ids = [
            $this->seedUser('U3', 'upd-in-3@example.com'),
            $this->seedUser('U4', 'upd-in-4@example.com'),
        ];
        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], 'id IN (?)', [$ids]);
        $this->assertSame(2, $rows);
    }

    public function testDocUpdateSqlTail(): void
    {
        $this->seedUser('Inactive', 'upd-tail-1@example.com', ['status' => 'inactive']);
        $this->seedUser('Inactive', 'upd-tail-2@example.com', ['status' => 'inactive']);
        $rows = $this->db->update(
            self::TABLE_USERS,
            ['status' => 'archived'],
            ['status' => 'inactive'],
            null,
            'ORDER BY id LIMIT 1'
        );
        $this->assertSame(1, $rows);
        $this->assertSame(1, (int) $this->pdo->query(
            'SELECT COUNT(*) FROM ' . self::TABLE_USERS . " WHERE status = 'archived'"
        )->fetchColumn());
    }

    public function testDocUpdateMultiBatch(): void
    {
        $since = '2024-01-01 00:00:00';
        $teamIds = [];
        foreach ([10, 11, 12] as $n) {
            $teamIds[] = $this->seedUser("Team{$n}", "team{$n}@example.com", [
                'email_verified' => 1,
                'views' => 0,
                'created_at' => '2024-06-01 00:00:00',
            ]);
        }
        $editorId = $this->seedUser('Editor', 'editor@example.com', ['role' => 'editor', 'views' => 0]);
        $carolId = $this->seedUser('Before Carol', 'carol@example.com', ['views' => 0]);
        $daveId = $this->seedUser('Before Dave', 'dave@example.com', [
            'views' => 0,
            'created_at' => '2024-06-01 00:00:00',
        ]);

        $rows = $this->db->update(self::TABLE_USERS, [
            ['name' => 'Alice', 'views = views + 1'],
            ['name' => 'Carol'],
            ['name' => 'Bob', 'views = views + ?' => 3],
            ['name' => 'Dave'],
        ], [
            [
                'id' => $teamIds[0],
                'email_verified != 0',
                'created_at > :since',
                'id IN (:ids)',
            ],
            ['id' => $carolId],
            'id = ? AND role = ?',
            'id = ' . $daveId . ' AND created_at > ?',
        ], [
            [':since' => $since, ':ids' => $teamIds],
            null,
            [$editorId, 'editor'],
            $since,
        ]);

        $this->assertGreaterThanOrEqual(4, $rows);
        $this->assertSame('Alice', $this->pdo->query(
            'SELECT name FROM ' . self::TABLE_USERS . ' WHERE id = ' . $teamIds[0]
        )->fetchColumn());
        $this->assertSame('Carol', $this->pdo->query(
            'SELECT name FROM ' . self::TABLE_USERS . ' WHERE id = ' . $carolId
        )->fetchColumn());
        $this->assertSame('Bob', $this->pdo->query(
            'SELECT name FROM ' . self::TABLE_USERS . ' WHERE id = ' . $editorId
        )->fetchColumn());
        $this->assertSame('Dave', $this->pdo->query(
            'SELECT name FROM ' . self::TABLE_USERS . ' WHERE id = ' . $daveId
        )->fetchColumn());
    }

    public function testDocUpdateAdvancedAssociativeBindings(): void
    {
        $status = 'active';
        $date = '2024-01-01 00:00:00';
        $id = $this->seedUser('Before', 'upd-adv@example.com', [
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);

        $rows = $this->db->update(self::TABLE_USERS, ['name' => 'John'], [
            'status' => $status,
            'email_verified != 0',
            'created_at > :date',
            'created_at > ?' => $date,
        ], [':date' => $date]);

        $this->assertSame(1, $rows);
    }

    // ——— select() / selectAll() doc examples ———

    public function testDocSelectClassicKeyValue(): void
    {
        $id = $this->seedUser('Select', 'select-classic@example.com');
        $row = $this->db->select(self::TABLE_USERS, 'id, name', 'id', $id)->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Select', $row['name']);
    }

    public function testDocSelectWithSqlTail(): void
    {
        $this->seedUser('Zebra', 'select-tail-z@example.com', ['status' => 'active']);
        $this->seedUser('Alpha', 'select-tail-a@example.com', ['status' => 'active']);
        $row = $this->db->select(
            self::TABLE_USERS,
            'id, name',
            'status',
            'active',
            'ORDER BY name LIMIT 1'
        )->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alpha', $row['name']);
    }

    public function testDocSelectAssociativeWhere(): void
    {
        $this->seedUser('Select', 'select-assoc@example.com', ['email_verified' => 1]);
        $row = $this->db->select(self::TABLE_USERS, 'id, name', [
            'status' => 'active',
            'email_verified' => 1,
        ])->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Select', $row['name']);
    }

    public function testDocSelectAssociativeWhereWithRawSql(): void
    {
        $date = '2024-01-01 00:00:00';
        $this->seedUser('Select', 'select-raw@example.com', [
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);
        $row = $this->db->select(self::TABLE_USERS, 'id, name', [
            'status' => 'active',
            'email_verified != 0',
            'created_at > :date',
        ], [':date' => $date])->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Select', $row['name']);
    }

    public function testDocSelectCustomWhereLastLogin(): void
    {
        $this->seedUser('Select', 'select-login@example.com', [
            'last_login' => '2022-06-01 00:00:00',
            'status' => 'active',
        ]);
        $row = $this->db->select(
            self::TABLE_USERS,
            'id, name',
            'last_login < ? AND status != ?',
            ['2023-01-01', 'inactive']
        )->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Select', $row['name']);
    }

    public function testDocSelectMixedPlaceholders(): void
    {
        $id = $this->seedUser('Select', 'select-mixed@example.com');
        $rows = $this->db->select(
            self::TABLE_USERS,
            'id, name',
            'status = :status AND id > ?',
            [':status' => 'active', $id - 1]
        )->fetchAll(PDO::FETCH_ASSOC);
        $this->assertNotEmpty($rows);
    }

    public function testDocSelectInNamedPlaceholder(): void
    {
        $ids = [
            $this->seedUser('S1', 's1@example.com'),
            $this->seedUser('S2', 's2@example.com'),
        ];
        $row = $this->db->select(self::TABLE_USERS, 'id, name', 'id IN (:ids)', [':ids' => $ids])
            ->fetch(PDO::FETCH_ASSOC);
        $this->assertContains($row['name'], ['S1', 'S2']);
    }

    public function testDocSelectInQuestionMarkPlaceholder(): void
    {
        $ids = [
            $this->seedUser('S3', 's3@example.com'),
            $this->seedUser('S4', 's4@example.com'),
        ];
        $rows = $this->db->select(self::TABLE_USERS, 'id, name', 'id IN (?)', [$ids])
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testDocSelectAdvancedAssociativeBindings(): void
    {
        $status = 'active';
        $date = '2024-01-01 00:00:00';
        $this->seedUser('Select', 'select-adv@example.com', [
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);
        $rows = $this->db->select(self::TABLE_USERS, 'id, name', [
            'status' => $status,
            'email_verified != 0',
            'created_at > :date',
            'created_at > ?' => $date,
        ], [':date' => $date])->fetchAll(PDO::FETCH_ASSOC);
        $this->assertNotEmpty($rows);
    }

    // ——— delete() doc examples ———

    public function testDocDeleteClassicKeyValue(): void
    {
        $id = $this->seedUser('Delete', 'delete-classic@example.com');
        $this->assertSame(1, $this->db->delete(self::TABLE_USERS, 'id', $id));
        $this->assertFalse($this->db->exists(self::TABLE_USERS, 'id', $id));
    }

    public function testDocDeleteAssociativeWhere(): void
    {
        $this->seedUser('Delete', 'delete-assoc@example.com', [
            'status' => 'inactive',
            'email_verified' => 0,
        ]);
        $rows = $this->db->delete(self::TABLE_USERS, [
            'status' => 'inactive',
            'email_verified' => 0,
        ]);
        $this->assertSame(1, $rows);
    }

    public function testDocDeleteAssociativeWhereWithRawSql(): void
    {
        $date = '2024-01-01 00:00:00';
        $this->seedUser('Delete', 'delete-raw@example.com', [
            'status' => 'inactive',
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);
        $rows = $this->db->delete(self::TABLE_USERS, [
            'status' => 'inactive',
            'email_verified != 0',
            'created_at > :date',
        ], [':date' => $date]);
        $this->assertSame(1, $rows);
    }

    public function testDocDeleteCustomWhereLastLogin(): void
    {
        $this->seedUser('Delete', 'delete-login@example.com', [
            'last_login' => '2022-01-01 00:00:00',
            'status' => 'active',
        ]);
        $rows = $this->db->delete(
            self::TABLE_USERS,
            'last_login < ? AND status != ?',
            ['2023-01-01', 'inactive']
        );
        $this->assertSame(1, $rows);
    }

    public function testDocDeleteMixedPlaceholders(): void
    {
        $id = $this->seedUser('Delete', 'delete-mixed@example.com', ['status' => 'inactive']);
        $rows = $this->db->delete(
            self::TABLE_USERS,
            'status = :status AND id > ?',
            [':status' => 'inactive', $id - 1]
        );
        $this->assertSame(1, $rows);
    }

    public function testDocDeleteInNamedPlaceholder(): void
    {
        $ids = [
            $this->seedUser('D1', 'd1@example.com'),
            $this->seedUser('D2', 'd2@example.com'),
        ];
        $this->assertSame(2, $this->db->delete(self::TABLE_USERS, 'id IN (:ids)', [':ids' => $ids]));
    }

    // ——— README Key Features ———

    public function testDocReadmeAutoQuestionMarkConversion(): void
    {
        $this->seedUser('Readme', 'user@example.com', ['status' => 'active']);
        $users = $this->db->selectAll(
            self::TABLE_USERS,
            null,
            'email = ? AND status = ?',
            ['user@example.com', 'active']
        );
        $this->assertCount(1, $users);
    }

    public function testDocReadmeInNamedPlaceholder(): void
    {
        $ids = [
            $this->seedUser('R1', 'r1@example.com'),
            $this->seedUser('R2', 'r2@example.com'),
        ];
        $users = $this->db->selectAll(self::TABLE_USERS, null, 'id IN (:ids)', [':ids' => $ids]);
        $this->assertCount(2, $users);
    }

    public function testDocReadmeInQuestionMarkPlaceholder(): void
    {
        $ids = [
            $this->seedUser('R3', 'r3@example.com'),
            $this->seedUser('R4', 'r4@example.com'),
        ];
        $users = $this->db->selectAll(self::TABLE_USERS, null, 'id IN (?)', [$ids]);
        $this->assertCount(2, $users);
    }

    public function testDocReadmeWhereArrayWithRawSqlNumericKeys(): void
    {
        $this->seedUser('Readme', 'readme-raw@example.com', [
            'email_verified' => 1,
            'created_at' => '2024-06-01 00:00:00',
        ]);
        $users = $this->db->selectAll(self::TABLE_USERS, null, [
            'status' => 'active',
            'email_verified != 0',
            'created_at > :date',
        ], [':date' => '2024-01-01']);
        $this->assertIsArray($users);
    }

    public function testDocReadmeWhereArrayWithQuestionMarkKey(): void
    {
        $this->seedUser('Readme', 'readme-qkey@example.com', ['created_at' => '2024-06-01 00:00:00']);
        $users = $this->db->selectAll(self::TABLE_USERS, null, [
            'status' => 'active',
            'created_at > ?' => '2024-01-01',
        ]);
        $this->assertNotEmpty($users);
    }

    public function testDocReadmeMixedPlaceholdersInWhereString(): void
    {
        $id = $this->seedUser('Readme', 'readme-mixed@example.com');
        $users = $this->db->selectAll(
            self::TABLE_USERS,
            null,
            'status = :status AND id > ?',
            [':status' => 'active', $id - 1]
        );
        $this->assertNotEmpty($users);
    }

    // ——— README Common Operations ———

    public function testDocReadmeSelectOneAndSelectAll(): void
    {
        $id = $this->seedUser('Readme', 'readme-select@example.com');
        $user = $this->db->selectOne(self::TABLE_USERS, ['name', 'email'], 'id', $id);
        $this->assertSame('Readme', $user['name']);

        $users = $this->db->selectAll(self::TABLE_USERS, ['name', 'email'], ['status' => 'active']);
        $this->assertNotEmpty($users);
    }

    public function testDocReadmeSelectAllWithSqlTail(): void
    {
        $this->seedUser('Zed', 'zed@example.com', ['status' => 'active']);
        $this->seedUser('Amy', 'amy@example.com', ['status' => 'active']);
        $users = $this->db->selectAll(
            self::TABLE_USERS,
            ['name'],
            ['status' => 'active'],
            null,
            'ORDER BY name LIMIT 10'
        );
        $this->assertLessThanOrEqual(10, count($users));
        $this->assertSame('Amy', $users[0]['name']);
    }

    public function testDocReadmeUpdateWithRawSqlInSet(): void
    {
        $id = $this->seedUser('Readme', 'readme-upd-raw@example.com');
        $rows = $this->db->update(self::TABLE_USERS, [
            'name' => 'John',
            'date_verified = NOW()',
        ], 'id', $id);
        $this->assertSame(1, $rows);
    }

    public function testDocReadmeUpdateBatchLoopPattern(): void
    {
        $rows = [
            ['id' => $this->seedUser('Loop A', 'loop-a@example.com'), 'name' => 'New A', 'status' => 'pending'],
            ['id' => $this->seedUser('Loop B', 'loop-b@example.com'), 'name' => 'New B', 'status' => 'pending'],
        ];
        $data = [];
        $where = [];
        foreach ($rows as $row) {
            $data[] = ['name' => $row['name'], 'status' => $row['status']];
            $where[] = ['id' => $row['id']];
        }
        $this->assertSame(2, $this->db->update(self::TABLE_USERS, $data, $where));
    }

    public function testDocReadmeDeleteOperations(): void
    {
        $id = $this->seedUser('Readme Del', 'readme-del@example.com');
        $this->assertSame(1, $this->db->delete(self::TABLE_USERS, 'id', $id));

        $this->seedUser('Inactive', 'readme-del-inactive@example.com', [
            'status' => 'inactive',
            'email_verified' => 0,
        ]);
        $this->assertSame(1, $this->db->delete(self::TABLE_USERS, [
            'status' => 'inactive',
            'email_verified' => 0,
        ]));
    }

    public function testDocReadmeExistsOperations(): void
    {
        $id = $this->seedUser('Readme', 'readme-exists@example.com');
        $this->assertTrue($this->db->exists(self::TABLE_USERS, 'id', $id));
        $this->assertTrue($this->db->exists(self::TABLE_USERS, [
            'email' => 'readme-exists@example.com',
            'status' => 'active',
        ]));
    }

    // ——— README Complex Example ———

    public function testDocReadmeComplexSelectAll(): void
    {
        $maxLastLogin = '2024-06-01';
        $userIds = [
            $this->seedUser('Complex A', 'complex-a@example.com', [
                'created_at' => '2024-03-01 00:00:00',
            ]),
            $this->seedUser('Complex B', 'complex-b@example.com', [
                'created_at' => '2024-03-01 00:00:00',
            ]),
        ];

        $users = $this->db->selectAll(
            self::TABLE_USERS,
            ['id', 'name', 'email'],
            [
                'status' => 'active',
                'name != ""',
                'created_at > :min_date',
                'created_at < ?' => $maxLastLogin,
                'id IN (:user_ids)',
            ],
            [
                ':min_date' => '2024-01-01',
                ':user_ids' => $userIds,
            ],
            'ORDER BY name LIMIT 10'
        );

        $this->assertGreaterThanOrEqual(2, count($users));
    }

    // ——— README Limitations (literal ? in SQL) ———

    public function testDocReadmeLiteralQuestionMarkInSqlStringIsProblematic(): void
    {
        $this->seedUser('Marc?', 'marc-problem@example.com');
        // The `?` in the SQL string is treated as a placeholder — binding mismatch yields no match.
        $users = $this->db->selectAll(self::TABLE_USERS, null, ["name LIKE '%Marc?'"]);
        $this->assertCount(0, $users);
    }

    public function testDocReadmeLiteralQuestionMarkInStringWhereIsProblematic(): void
    {
        $this->seedUser('Marc?', 'marc-problem2@example.com', ['status' => 'active']);
        $this->expectException(PDOException::class);
        $this->db->selectAll(self::TABLE_USERS, null, "name LIKE '%Marc?' AND status = ?", ['active']);
    }

    public function testDocReadmeLiteralQuestionMarkSolutionStringWhere(): void
    {
        $this->seedUser('Marc?', 'marc-solution@example.com', ['status' => 'active']);
        $pattern = '%Marc?';
        $users = $this->db->selectAll(
            self::TABLE_USERS,
            null,
            'name LIKE ? AND status = ?',
            [$pattern, 'active']
        );
        $this->assertCount(1, $users);
    }

    public function testDocReadmeLiteralQuestionMarkSolutionArrayWhere(): void
    {
        $this->seedUser('Marc?', 'marc-solution2@example.com', ['status' => 'active']);
        $pattern = '%Marc?';
        $users = $this->db->selectAll(
            self::TABLE_USERS,
            null,
            ['name LIKE ?', 'status = ?'],
            [$pattern, 'active']
        );
        $this->assertCount(1, $users);
    }

    // ——— selectCompose() doc examples (adapted to doc_users / doc_profiles) ———

    public function testDocSelectComposeCoreColumnsFilterAndOrder(): void
    {
        $activeId = $this->seedUser('Compose Alice', 'compose-alice@example.com', ['status' => 'active']);
        $this->seedUser('Compose Bob', 'compose-bob@example.com', ['status' => 'pending']);

        $rows = $this->db->selectCompose(
            self::TABLE_USERS,
            [
                ['select' => ['id', 'name', 'status']],
                ['where' => ['status' => 'active']],
            ],
            'ORDER BY id'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame($activeId, (int)$rows[0]['id']);
        $this->assertSame('Compose Alice', $rows[0]['name']);
    }

    public function testDocSelectComposeOptionalFilterFragment(): void
    {
        $id1 = $this->seedUser('Opt A', 'opt-a@example.com', ['status' => 'active']);
        $this->seedUser('Opt B', 'opt-b@example.com', ['status' => 'active']);

        $core = ['select' => ['id', 'name']];
        $statusFrag = ['where' => ['status' => 'active']];
        $fragments = [$core, $statusFrag];
        // Optional person/id filter — only when needed.
        $filterToPerson = true;
        if ($filterToPerson) {
            $fragments[] = ['where' => ['id' => $id1]];
        }

        $rows = $this->db->selectCompose(self::TABLE_USERS, $fragments, 'ORDER BY id')
            ->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Opt A', $rows[0]['name']);
    }

    public function testDocSelectComposeRawWhereWithNamedBindings(): void
    {
        $id = $this->seedUser('Raw Named', 'raw-named@example.com', [
            'status' => 'active',
            'email_verified' => 1,
        ]);

        $rows = $this->db->selectCompose(self::TABLE_USERS, [
            ['select' => ['id', 'name']],
            [
                'where' => ['(status = :status OR email_verified = :verified)'],
                'bindings' => [':status' => 'active', ':verified' => 1],
            ],
            ['where' => ['id' => $id]],
        ])->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Raw Named', $rows[0]['name']);
    }

    public function testDocSelectComposeBindingsInSelectSubquery(): void
    {
        $id = $this->seedUser('Subwhat', 'subwhat@example.com');

        $rows = $this->db->selectCompose(self::TABLE_USERS, [
            [
                'select' => [
                    'id',
                    'name',
                    '(SELECT :subwhat) AS senden',
                ],
                'bindings' => [':subwhat' => 'flag'],
            ],
            ['where' => ['id' => $id]],
        ])->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('flag', $rows[0]['senden']);
    }

    public function testDocSelectComposeAlternateFromWithInList(): void
    {
        $ids = [
            $this->seedUser('In A', 'in-a@example.com', ['status' => 'active']),
            $this->seedUser('In B', 'in-b@example.com', ['status' => 'active']),
            $this->seedUser('In C', 'in-c@example.com', ['status' => 'pending']),
        ];
        $this->pdo->exec(
            'INSERT INTO ' . self::TABLE_PROFILES . ' (user_id, bio, status) VALUES '
            . "({$ids[0]}, 'bio-a', 'active'), ({$ids[1]}, 'bio-b', 'active')"
        );

        $from = self::TABLE_USERS . ' LEFT JOIN ' . self::TABLE_PROFILES
            . ' ON ' . self::TABLE_PROFILES . '.user_id = ' . self::TABLE_USERS . '.id';

        $rows = $this->db->selectCompose(
            $from,
            [
                ['select' => [self::TABLE_USERS . '.id', self::TABLE_USERS . '.name', self::TABLE_PROFILES . '.bio']],
                [
                    'where' => [self::TABLE_USERS . '.id IN (:ids)'],
                    'bindings' => [':ids' => [$ids[0], $ids[1]]],
                ],
            ],
            'ORDER BY ' . self::TABLE_USERS . '.id'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame(['bio-a', 'bio-b'], array_column($rows, 'bio'));
    }
}
