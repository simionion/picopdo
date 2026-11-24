<?php
declare(strict_types=1);

namespace Lodur\PicoPdo\Tests\Integration {

    use Lodur\PicoPdo\CommonModelPicoPdoTrait;
    use PDO;
    use PHPUnit\Framework\Attributes\DataProvider;
    use PHPUnit\Framework\TestCase;

    /**
     * Real MariaDB/MySQL proof for insert() ID ownership. One uniquely named InnoDB table per test,
     * created with plain CREATE TABLE (a name collision fails instead of touching another table) and
     * dropped in tearDown. Read-only probe of @@SESSION.max_allowed_packet; no SET GLOBAL, no root.
     */
    final class CommonModelPicoPdoInsertIdOwnershipIntegrationTest extends TestCase
    {
        private PDO $pdo;

        private object $model;

        private string $table;

        protected function setUp(): void
        {
            parent::setUp();
            $this->pdo = new PDO(
                'mysql:host=' . getenv('DB_HOST') . ';dbname=' . getenv('DB_NAME'),
                getenv('DB_USER'),
                getenv('DB_PASS'),
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_STRINGIFY_FETCHES => false]
            );
            $this->table = 'pico_insert_id_' . bin2hex(random_bytes(6));
            $this->pdo->exec("CREATE TABLE `{$this->table}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tag VARCHAR(16) NOT NULL UNIQUE,
                x INT NULL,
                y INT NULL,
                note VARCHAR(16) NULL,
                payload MEDIUMTEXT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            $this->model = new class ($this->pdo) {
                use CommonModelPicoPdoTrait {
                    insert as public;
                    insertReplace as public;
                }

                public function __construct(PDO $pdo)
                {
                    $this->pdo = $pdo;
                }
            };
        }

        protected function tearDown(): void
        {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            $this->pdo->exec("DROP TABLE IF EXISTS `{$this->table}`");
            parent::tearDown();
        }

        public static function prepareModes(): array
        {
            return ['emulated prepares' => [true], 'native prepares' => [false]];
        }

        /** Root-cause probe: on this driver a COMMIT reply resets the connection's insert ID. */
        public function testCommitResetsTheConnectionInsertId(): void
        {
            $this->pdo->beginTransaction();
            $this->pdo->exec("INSERT INTO `{$this->table}` (tag) VALUES ('probe')");
            $beforeCommit = $this->pdo->lastInsertId();
            $this->pdo->commit();

            $this->assertGreaterThan(0, (int)$beforeCommit);
            $this->assertSame('probe', $this->tagOf($beforeCommit));
            $this->assertSame('0', $this->pdo->lastInsertId(), 'if this fails the source hypothesis is falsified');
        }

        #[DataProvider('prepareModes')]
        public function testOwnedMixedShapeBatchReturnsTheFirstRowOfTheFinalStatement(bool $emulated): void
        {
            $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);

            $result = $this->model->insert($this->table, $this->mixedRows());

            $this->assertSame(3, $result['rows']);
            $this->assertSame('inserted', $result['status']);
            $this->assertIsInt($result['id']);
            $this->assertSame('b', $this->tagOf($result['id']), 'the final statement inserts only row b');
            $this->assertFalse($this->pdo->inTransaction());
            $this->assertSame(['a', 'b', 'c'], $this->tags());
        }

        #[DataProvider('prepareModes')]
        public function testCallerTransactionIsRetainedAndItsRollbackStillWorks(bool $emulated): void
        {
            $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
            $this->pdo->beginTransaction();

            $result = $this->model->insert($this->table, $this->mixedRows());

            $this->assertTrue($this->pdo->inTransaction());
            $this->assertSame(3, $result['rows']);
            $this->assertSame('b', $this->tagOf($result['id']));

            $this->pdo->rollBack();
            $this->assertSame([], $this->tags());
        }

        #[DataProvider('prepareModes')]
        public function testScalarAndSameShapeListIdsNameTheirRows(bool $emulated): void
        {
            $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);

            $id = $this->model->insert($this->table, ['tag' => 'solo', 'x' => 1]);
            $this->assertIsInt($id);
            $this->assertSame('solo', $this->tagOf($id));

            $list = $this->model->insert($this->table, [['tag' => 'l1', 'x' => 1], ['tag' => 'l2', 'x' => 2]]);
            $this->assertSame(2, $list['rows']);
            $this->assertSame('l1', $this->tagOf($list['id']), 'a multi-row INSERT reports its first generated ID');
        }

        public static function freshModes(): array
        {
            $cases = [];
            foreach (['emulated' => true, 'native' => false] as $label => $emulated) {
                $cases["{$label} REPLACE"] = [$emulated, 'replace', 'inserted'];
                $cases["{$label} INSERT IGNORE"] = [$emulated, 'ignore', 'inserted'];
                $cases["{$label} upsert"] = [$emulated, 'upsert', 'affected'];
            }
            return $cases;
        }

        #[DataProvider('freshModes')]
        public function testEveryModeReturnsTheFinalStatementRowInsideAnOwnedTransaction(bool $emulated, string $mode, string $status): void
        {
            $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);

            $result = $this->write($mode);

            $this->assertSame(['rows' => 3, 'status' => $status], ['rows' => $result['rows'], 'status' => $result['status']]);
            $this->assertSame('b', $this->tagOf($result['id']));
            $this->assertFalse($this->pdo->inTransaction());
        }

        public static function existingFinalRowModes(): array
        {
            return [
                'emulated INSERT IGNORE' => [true, 'ignore'],
                'native INSERT IGNORE'   => [false, 'ignore'],
                'emulated upsert'        => [true, 'upsert'],
                'native upsert'          => [false, 'upsert'],
            ];
        }

        /**
         * The final statement meets an existing row (ignored / updated). Whatever the server reports for
         * that statement, the metadata must not depend on who owns the transaction.
         */
        #[DataProvider('existingFinalRowModes')]
        public function testFinalStatementOutcomeIsIndependentOfTransactionOwnership(bool $emulated, string $mode): void
        {
            $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
            $this->pdo->exec("INSERT INTO `{$this->table}` (tag, y) VALUES ('b', 0)");

            $this->pdo->beginTransaction();
            $callerOwned = $this->write($mode);
            $callerIdentity = $this->tagOf($callerOwned['id']);
            $this->pdo->rollBack();

            $owned = $this->write($mode);
            $ownedIdentity = $this->tagOf($owned['id']);

            $this->assertSame([$callerOwned['rows'], $callerOwned['status']], [$owned['rows'], $owned['status']]);
            $this->assertSame($callerIdentity, $ownedIdentity);
            $this->assertFalse($this->pdo->inTransaction());
            $this->assertSame(['a', 'b', 'c'], $this->tags());
        }

        public function testPacketChunkedBatchReturnsTheFirstRowOfTheFinalChunk(): void
        {
            $packet = (int)$this->pdo->query('SELECT @@SESSION.max_allowed_packet')->fetchColumn();
            if ($packet < 8 * 1024 * 1024) {
                $this->markTestSkipped("max_allowed_packet {$packet} is below the ~4.6 MiB chunk this case sends");
            }
            $rows = [];
            foreach (['p0', 'p1', 'p2', 'p3'] as $tag) {
                $rows[] = ['tag' => $tag, 'payload' => str_repeat('P', 2_300_000)];
            }

            $result = $this->model->insert($this->table, $rows);

            $this->assertSame(4, $result['rows']);
            $this->assertSame('p2', $this->tagOf($result['id']), 'chunks are [p0,p1] then [p2,p3]');
            $this->assertFalse($this->pdo->inTransaction());
            $this->assertSame(['p0', 'p1', 'p2', 'p3'], $this->tags());
        }

        /** @return list<array<string, mixed>> statements: [a, c] (tag|x) then [b] (tag|y) */
        private function mixedRows(): array
        {
            return [['tag' => 'a', 'x' => 1], ['tag' => 'b', 'y' => 2], ['tag' => 'c', 'x' => 3]];
        }

        private function write(string $mode): array
        {
            return match ($mode) {
                'replace' => $this->model->insertReplace($this->table, $this->mixedRows()),
                'ignore'  => $this->model->insert($this->table, $this->mixedRows(), ['mode' => 'INSERT IGNORE']),
                'upsert'  => $this->model->insert($this->table, $this->mixedRows(), ['onDuplicateKeyUpdate' => ['note' => 'u']]),
            };
        }

        private function tagOf(int|string $id): string|null
        {
            if ((int)$id === 0) {
                return null;
            }
            $statement = $this->pdo->prepare("SELECT tag FROM `{$this->table}` WHERE id = ?");
            $statement->execute([(int)$id]);
            $tag = $statement->fetchColumn();
            return $tag === false ? null : $tag;
        }

        /** @return list<string> */
        private function tags(): array
        {
            return $this->pdo->query("SELECT tag FROM `{$this->table}` ORDER BY tag")->fetchAll(PDO::FETCH_COLUMN);
        }
    }
}
