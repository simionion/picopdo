<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests\Integration;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class CommonModelPicoPdoQuotedInsertTest extends TestCase
{
    private const TABLE = 'test_quoted_insert';

    private PDO $pdo;

    private object $model;

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo = new PDO(
            'mysql:host=' . getenv('DB_HOST') . ';dbname=' . getenv('DB_NAME'),
            getenv('DB_USER'),
            getenv('DB_PASS'),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_STRINGIFY_FETCHES => false]
        );
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->pdo->exec('CREATE TABLE ' . self::TABLE . ' (
            id INT AUTO_INCREMENT PRIMARY KEY,
            `key` VARCHAR(64) NOT NULL UNIQUE,
            `rows` INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            note TEXT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
        $this->model = new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                insert as public;
                insertReplace as public;
                insertIgnore as public;
                insertOnDuplicateKeyUpdate as public;
                selectOne as public;
                selectAll as public;
            }

            public function __construct(private PDO $connection)
            {
            }
        };
    }

    protected function tearDown(): void
    {
        if ($this->pdo->inTransaction()) {
            $this->pdo->rollBack();
        }
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        parent::tearDown();
    }

    public static function prepareModes(): array
    {
        return ['emulated prepares' => [true], 'native prepares' => [false]];
    }

    #[DataProvider('prepareModes')]
    public function testQuotedSingleRowPreservesValuesAndGeneratedId(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $name = "L'équipe O'Brien ? :name `literal` \\";
        $id = $this->model->insert(self::TABLE, [
            '`key`' => 'single',
            '`rows`' => 7,
            'name' => $name,
            '`note`' => null,
        ]);

        $this->assertIsInt($id);
        $this->assertGreaterThan(0, $id);
        $row = $this->model->selectOne(self::TABLE, '*', 'id', $id);
        $this->assertSame($id, $row['id']);
        $this->assertSame('single', $row['key']);
        $this->assertSame(7, $row['rows']);
        $this->assertSame($name, $row['name']);
        $this->assertNull($row['note']);
    }

    #[DataProvider('prepareModes')]
    public function testQuotedBatchKeepsEveryRowsBindingsDistinct(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $result = $this->model->insert(self::TABLE, [
            [],
            ['`key`' => 'first', '`rows`' => 0, 'name' => 'A', '`note`' => null],
            ['`key`' => 'second', '`rows`' => -1, 'name' => 'B', '`note`' => 'NULL'],
            ['`key`' => 'third', '`rows`' => 42, 'name' => 'C', '`note`' => '007'],
        ]);

        $this->assertSame(3, $result['rows']);
        $this->assertSame('inserted', $result['status']);
        $this->assertIsInt($result['id']);
        $this->assertGreaterThan(0, $result['id']);
        $this->assertSame([
            ['key' => 'first', 'rows' => 0, 'name' => 'A', 'note' => null],
            ['key' => 'second', 'rows' => -1, 'name' => 'B', 'note' => 'NULL'],
            ['key' => 'third', 'rows' => 42, 'name' => 'C', 'note' => '007'],
        ], $this->model->selectAll(self::TABLE, '`key`, `rows`, name, note', null, null, 'ORDER BY id'));
    }

    #[DataProvider('prepareModes')]
    public function testQuotedBindingsRemainDistinctBetweenInsertShapes(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $result = $this->model->insert(self::TABLE, [
            ['`key`' => 'first', '`rows`' => 1, 'name' => 'A', '`note`' => 'left'],
            ['`key`' => 'second', '`rows`' => 2, 'name' => 'B', 'created_at = NOW()'],
            ['`key`' => 'third', '`rows`' => 3, 'name' => 'C', '`note`' => 'right'],
        ]);

        $this->assertSame(3, $result['rows']);
        $this->assertSame('inserted', $result['status']);
        $this->assertFalse($this->pdo->inTransaction());
        $this->assertSame([
            ['key' => 'first', 'rows' => 1, 'name' => 'A', 'note' => 'left'],
            ['key' => 'second', 'rows' => 2, 'name' => 'B', 'note' => null],
            ['key' => 'third', 'rows' => 3, 'name' => 'C', 'note' => 'right'],
        ], $this->model->selectAll(self::TABLE, '`key`, `rows`, name, note', null, null, 'ORDER BY `key`'));
    }

    public static function writeModes(): array
    {
        return [
            'emulated REPLACE' => [true, 'replace'],
            'native REPLACE' => [false, 'replace'],
            'emulated INSERT IGNORE' => [true, 'ignore'],
            'native INSERT IGNORE' => [false, 'ignore'],
            'emulated upsert' => [true, 'upsert'],
            'native upsert' => [false, 'upsert'],
        ];
    }

    #[DataProvider('writeModes')]
    public function testQuotedKeysWorkWithEveryInsertMode(bool $emulated, string $mode): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $write = fn(array $data): int|string|array => match ($mode) {
            'replace' => $this->model->insertReplace(self::TABLE, $data),
            'ignore' => $this->model->insertIgnore(self::TABLE, $data),
            'upsert' => $this->model->insertOnDuplicateKeyUpdate(self::TABLE, $data, [
                '`rows` = ?' => 2,
                'name' => 'after',
            ]),
        };
        $created = $write(['`key`' => 'mode', '`rows`' => 1, 'name' => 'before']);
        $id = is_array($created) ? $created['id'] : $created;
        $this->assertIsInt($id);
        $this->assertGreaterThan(0, $id);
        if (is_array($created)) {
            $this->assertSame(1, $created['rows']);
            $this->assertSame('inserted', $created['status']);
        }

        $result = $write(['`key`' => 'mode', '`rows`' => 2, 'name' => 'after']);
        if ($mode === 'replace') {
            $this->assertIsInt($result);
            $this->assertGreaterThan($id, $result);
        } elseif ($mode === 'ignore') {
            $this->assertSame(['id' => 0, 'rows' => 0, 'status' => 'noop'], $result);
        } else {
            $this->assertSame(2, $result['rows']);
            $this->assertSame('updated', $result['status']);
        }
        $expected = $mode === 'ignore'
            ? ['key' => 'mode', 'rows' => 1, 'name' => 'before']
            : ['key' => 'mode', 'rows' => 2, 'name' => 'after'];
        $this->assertSame([$expected], $this->model->selectAll(self::TABLE, '`key`, `rows`, name'));
    }

    #[DataProvider('prepareModes')]
    public function testQuotedMultiStatementInsertRollsBackWhenALaterShapeFails(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        try {
            $this->model->insert(self::TABLE, [
                ['`key`' => 'duplicate', '`rows`' => 1, 'name' => 'first', '`note`' => 'first shape'],
                ['`key`' => 'duplicate', '`rows`' => 2, 'name' => 'second'],
            ]);
            $this->fail('A duplicate unique key must fail the second insert shape');
        } catch (PDOException $failure) {
            $this->assertSame('23000', $failure->getCode());
        }

        $this->assertFalse($this->pdo->inTransaction());
        $this->assertSame([], $this->model->selectAll(self::TABLE));
    }
}
