<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests\Integration;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;

/** Transaction units on a real connection: owned transactions, savepoints and what each rollback undoes. */
class CommonModelPicoPdoTransactionIntegrationTest extends TestCase
{
    private const TABLE = 'test_transaction_units';

    private PDO $pdo;

    private object $model;

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo = new PDO(
            'mysql:host=' . getenv('DB_HOST') . ';dbname=' . getenv('DB_NAME'),
            getenv('DB_USER'),
            getenv('DB_PASS'),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->pdo->exec(
            'CREATE TABLE ' . self::TABLE . ' (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL) ENGINE=InnoDB'
        );
        $this->model = new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                insert as public;
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

    public function testAnOwnedUnitCommitsItsWrites(): void
    {
        $id = $this->model->runInTransaction(fn(): int => (int) $this->model->insert(self::TABLE, ['name' => 'kept']));

        $this->assertGreaterThan(0, $id);
        $this->assertFalse($this->pdo->inTransaction());
        $this->assertSame(['kept'], $this->names());
    }

    public function testARejectedOwnedUnitIsRolledBackAndItsResultReturned(): void
    {
        $result = $this->model->runInTransaction(function (): array {
            $this->model->insert(self::TABLE, ['name' => 'dropped']);
            return ['status' => false, 'error' => 'rejected'];
        }, static fn(array $result): bool => !$result['status']);

        $this->assertSame(['status' => false, 'error' => 'rejected'], $result);
        $this->assertFalse($this->pdo->inTransaction());
        $this->assertSame([], $this->names());
    }

    public function testNestedUnitsUndoOnlyTheirOwnSavepoint(): void
    {
        $this->pdo->beginTransaction();
        $this->model->insert(self::TABLE, ['name' => 'outer']);

        $this->model->runInTransaction(fn(): int => (int) $this->model->insert(self::TABLE, ['name' => 'inner kept']));
        $this->model->runInTransaction(function (): bool {
            $this->model->insert(self::TABLE, ['name' => 'inner rejected']);
            return false;
        }, static fn(bool $isSaved): bool => !$isSaved);
        try {
            $this->model->runInTransaction(function (): void {
                $this->model->insert(self::TABLE, ['name' => 'inner failed']);
                throw new PDOException('unit failed');
            });
            $this->fail('The nested failure must propagate');
        } catch (PDOException $failure) {
            $this->assertSame('unit failed', $failure->getMessage());
        }

        $this->assertTrue($this->pdo->inTransaction(), 'the caller still owns its transaction');
        $this->pdo->commit();
        $this->assertSame(['outer', 'inner kept'], $this->names());
    }

    public function testIsInTransactionFollowsTheConnection(): void
    {
        $this->assertFalse($this->model->isInTransaction());
        $this->pdo->beginTransaction();
        $this->assertTrue($this->model->isInTransaction());
        $this->pdo->rollBack();
        $this->assertFalse($this->model->isInTransaction());
    }

    /** @return list<string> */
    private function names(): array
    {
        return $this->pdo->query('SELECT name FROM ' . self::TABLE . ' ORDER BY id')->fetchAll(PDO::FETCH_COLUMN);
    }
}
