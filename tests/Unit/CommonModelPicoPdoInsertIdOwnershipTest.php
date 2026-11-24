<?php
/**
 * 119439 — regression candidates for insert() generated-ID ownership. NOT EXECUTED by the author.
 *
 * PHPUnit maps one class per file, so the lead splits this file into:
 *   - namespace Lodur\PicoPdo\Tests             → tests/Unit/CommonModelPicoPdoInsertIdOwnershipTest.php
 *   - namespace Lodur\PicoPdo\Tests\Integration → tests/Integration/CommonModelPicoPdoInsertIdOwnershipIntegrationTest.php
 * (each file keeps `declare(strict_types=1);` and its own namespace block / `use` list).
 *
 * Unit class: no database. A mocked PDO models the pdo_mysql/mysqlnd rule that the ID belongs to the
 * connection's latest server reply: an INSERT reports the first ID it generated (0 if it inserted
 * nothing) and BEGIN/COMMIT/ROLLBACK replies reset it to 0. It records the exact call order.
 *
 * Integration class: proposal for the lead to run with the repository suite against its supplied test
 * connection (same DB_* convention as every existing integration test). It owns one uniquely named
 * InnoDB table per test, never changes server/global settings, and drops only that table.
 *
 * Expected on the ORIGINAL trait (red): the owned multi-statement cases return id 0.
 * Expected on the candidate (green): all cases. Caller-owned, scalar, same-shape, no-op and failure
 * cases are semantic guards and should pass on both.
 */

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests {

    use Lodur\PicoPdo\CommonModelPicoPdoTrait;
    use Lodur\PicoPdo\CommonModelPicoPdoUtils;
    use PDO;
    use PDOException;
    use PDOStatement;
    use PHPUnit\Framework\Attributes\DataProvider;
    use PHPUnit\Framework\TestCase;
    use stdClass;

    final class CommonModelPicoPdoInsertIdOwnershipTest extends TestCase
    {
        /** Three rows, two shapes: statements are [a, c] (tag|x) then [b] (tag|y) — the S08 shape. */
        private const MIXED_ROWS = [
            ['tag' => 'a', 'x' => 1],
            ['tag' => 'b', 'y' => 2],
            ['tag' => 'c', 'x' => 3],
        ];

        public function testOwnedMixedShapeBatchReturnsTheFinalStatementIdReadBeforeCommit(): void
        {
            [$pdo, $server] = $this->connection();

            $result = $this->model($pdo)->insert('t', self::MIXED_ROWS);

            // a=101, c=102 (first statement), b=103 (final statement) — the ID names row b.
            $this->assertSame(['id' => 103, 'rows' => 3, 'status' => 'inserted'], $result);
            $this->assertSame('b', $server->table[$result['id']]);
            $this->assertSame(['BEGIN', 'EXECUTE a,c', 'EXECUTE b', 'LAST_INSERT_ID', 'COMMIT'], $server->log);
            $this->assertFalse($pdo->inTransaction());
            // The cause: once the owned COMMIT has replied, the connection reports 0.
            $this->assertSame('0', $pdo->lastInsertId());
        }

        public function testCallerTransactionIsRetainedAndReportsTheSameFinalStatementId(): void
        {
            [$pdo, $server] = $this->connection();
            $pdo->beginTransaction();

            $result = $this->model($pdo)->insert('t', self::MIXED_ROWS);

            $this->assertSame(['id' => 103, 'rows' => 3, 'status' => 'inserted'], $result);
            $this->assertSame('b', $server->table[$result['id']]);
            $this->assertSame(['BEGIN', 'EXECUTE a,c', 'EXECUTE b', 'LAST_INSERT_ID'], $server->log);
            $this->assertTrue($pdo->inTransaction(), 'insert() must not commit a caller-owned transaction');

            $pdo->rollBack();
            $this->assertSame([], $server->table, 'the caller still owns the rollback');
        }

        public function testScalarInsertRunsWithoutATransactionAndReturnsItsId(): void
        {
            [$pdo, $server] = $this->connection();

            $id = $this->model($pdo)->insert('t', ['tag' => 'solo']);

            $this->assertSame(101, $id);
            $this->assertSame('solo', $server->table[$id]);
            $this->assertSame(['EXECUTE solo', 'LAST_INSERT_ID'], $server->log);
        }

        public function testSameShapeListIsOneStatementAndReportsItsFirstGeneratedId(): void
        {
            [$pdo, $server] = $this->connection();

            $result = $this->model($pdo)->insert('t', [['tag' => 'a'], ['tag' => 'b'], ['tag' => 'c']]);

            $this->assertSame(['id' => 101, 'rows' => 3, 'status' => 'inserted'], $result);
            $this->assertSame('a', $server->table[$result['id']]);
            $this->assertSame(['EXECUTE a,b,c', 'LAST_INSERT_ID'], $server->log);
        }

        public function testPacketChunkedBatchReportsTheFirstIdOfTheFinalChunk(): void
        {
            [$pdo, $server] = $this->connection();
            $rows = [];
            foreach (['p0', 'p1', 'p2', 'p3'] as $tag) {
                $rows[] = ['tag' => $tag, 'payload' => str_repeat('P', 2_300_000)];
            }
            $budget = CommonModelPicoPdoUtils::MAX_ALLOWED_PACKET * CommonModelPicoPdoUtils::PACKET_FILL_RATIO;
            $this->assertGreaterThan($budget, strlen(serialize($rows)), 'precondition: the batch must split');
            $this->assertLessThan($budget, strlen(serialize(array_slice($rows, 0, 2))), 'precondition: halves fit');

            $result = $this->model($pdo)->insert('t', $rows);

            $this->assertSame(['id' => 103, 'rows' => 4, 'status' => 'inserted'], $result);
            $this->assertSame('p2', $server->table[$result['id']]);
            $this->assertSame(['BEGIN', 'EXECUTE p0,p1', 'EXECUTE p2,p3', 'LAST_INSERT_ID', 'COMMIT'], $server->log);
        }

        public static function insertModes(): array
        {
            return [
                'INSERT'        => ['insert', 'INSERT INTO', 'inserted'],
                'REPLACE'       => ['replace', 'REPLACE INTO', 'inserted'],
                'INSERT IGNORE' => ['ignore', 'INSERT IGNORE INTO', 'inserted'],
                'upsert'        => ['upsert', 'INSERT INTO', 'affected'],
            ];
        }

        #[DataProvider('insertModes')]
        public function testEveryModeReportsTheFinalStatementIdInsideAnOwnedTransaction(string $mode, string $verb, string $status): void
        {
            [$pdo, $server] = $this->connection();

            $result = $this->write($this->model($pdo), $mode, self::MIXED_ROWS);

            $this->assertSame(['id' => 103, 'rows' => 3, 'status' => $status], $result);
            $this->assertSame('b', $server->table[$result['id']]);
            $this->assertSame(['BEGIN', 'EXECUTE a,c', 'EXECUTE b', 'LAST_INSERT_ID', 'COMMIT'], $server->log);
            foreach ($server->sql as $sql) {
                $this->assertStringStartsWith("{$verb} t ", $sql);
                $this->assertSame($mode === 'upsert', str_contains($sql, ' ON DUPLICATE KEY UPDATE '));
            }
        }

        public static function ownership(): array
        {
            return ['owned transaction' => [false], 'caller transaction' => [true]];
        }

        #[DataProvider('ownership')]
        public function testAFinalStatementThatInsertsNothingReportsZeroWhoeverOwnsTheTransaction(bool $callerOwned): void
        {
            [$pdo, $server] = $this->connection(ignored: ['b']);
            if ($callerOwned) {
                $pdo->beginTransaction();
            }

            $result = $this->write($this->model($pdo), 'ignore', self::MIXED_ROWS);

            // Last-INSERT semantics: the final statement generated no ID, so neither does the batch.
            $this->assertSame(['id' => 0, 'rows' => 2, 'status' => 'inserted'], $result);
            $this->assertSame([101 => 'a', 102 => 'c'], $server->table);
            $this->assertSame($callerOwned, $pdo->inTransaction());
        }

        #[DataProvider('ownership')]
        public function testAnAllNoopBatchIsNoopWhoeverOwnsTheTransaction(bool $callerOwned): void
        {
            [$pdo, $server] = $this->connection(ignored: ['a', 'b', 'c']);
            if ($callerOwned) {
                $pdo->beginTransaction();
            }

            $result = $this->write($this->model($pdo), 'ignore', self::MIXED_ROWS);

            $this->assertSame(['id' => 0, 'rows' => 0, 'status' => 'noop'], $result);
            $this->assertSame([], $server->table);
            $this->assertSame($callerOwned, $pdo->inTransaction());
        }

        public function testAFailingFinalStatementRollsBackTheOwnedUnitWithoutReadingAnId(): void
        {
            [$pdo, $server] = $this->connection(failing: ['b']);

            try {
                $this->model($pdo)->insert('t', self::MIXED_ROWS);
                $this->fail('The failing final statement must propagate');
            } catch (PDOException $failure) {
                $this->assertSame('simulated failure for b', $failure->getMessage());
            }

            $this->assertSame(['BEGIN', 'EXECUTE a,c', 'EXECUTE b', 'ROLLBACK'], $server->log);
            $this->assertSame([], $server->table);
            $this->assertFalse($pdo->inTransaction());
        }

        public function testAFailingFinalStatementLeavesTheCallerTransactionOpen(): void
        {
            [$pdo, $server] = $this->connection(failing: ['b']);
            $pdo->beginTransaction();

            try {
                $this->model($pdo)->insert('t', self::MIXED_ROWS);
                $this->fail('The failing final statement must propagate');
            } catch (PDOException) {
            }

            $this->assertSame(['BEGIN', 'EXECUTE a,c', 'EXECUTE b'], $server->log);
            $this->assertTrue($pdo->inTransaction());
        }

        public function testAnEmptyPayloadSendsNothing(): void
        {
            [$pdo, $server] = $this->connection();
            $model = $this->model($pdo);

            $this->assertSame(0, $model->insert('t', []));
            $this->assertSame(['id' => 0, 'rows' => 0, 'status' => 'noop'], $model->insert('t', [], ['meta' => true]));
            $this->assertSame([], $server->log);
        }

        private function write(object $model, string $mode, array $rows): int|string|array
        {
            return match ($mode) {
                'insert'  => $model->insert('t', $rows),
                'replace' => $model->insertReplace('t', $rows),
                'ignore'  => $model->insert('t', $rows, ['mode' => 'INSERT IGNORE']),
                'upsert'  => $model->insert('t', $rows, ['onDuplicateKeyUpdate' => ['note' => 'u']]),
            };
        }

        private function model(PDO $pdo): object
        {
            return new class ($pdo) {
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

        /**
         * @param list<string> $ignored Tags the server treats as duplicates (not inserted, no ID).
         * @param list<string> $failing Tags whose statement fails.
         * @return array{0: PDO, 1: stdClass}
         */
        private function connection(array $ignored = [], array $failing = []): array
        {
            $server = new stdClass();
            $server->autoIncrement = 100;
            $server->insertId = '0';
            $server->open = false;
            $server->table = [];
            $server->snapshot = [];
            $server->log = [];
            $server->sql = [];

            $pdo = $this->createMock(PDO::class);
            $pdo->method('inTransaction')->willReturnCallback(static fn(): bool => $server->open);
            $pdo->method('lastInsertId')->willReturnCallback(static function () use ($server): string {
                $server->log[] = 'LAST_INSERT_ID';
                return $server->insertId;
            });
            $pdo->method('beginTransaction')->willReturnCallback(static function () use ($server): bool {
                $server->open = true;
                $server->snapshot = $server->table;
                $server->insertId = '0';
                $server->log[] = 'BEGIN';
                return true;
            });
            $pdo->method('commit')->willReturnCallback(static function () use ($server): bool {
                $server->open = false;
                $server->insertId = '0';
                $server->log[] = 'COMMIT';
                return true;
            });
            $pdo->method('rollBack')->willReturnCallback(static function () use ($server): bool {
                $server->open = false;
                $server->table = $server->snapshot;
                $server->insertId = '0';
                $server->log[] = 'ROLLBACK';
                return true;
            });
            $pdo->method('prepare')->willReturnCallback(function (string $sql) use ($server, $ignored, $failing): PDOStatement {
                $bound = [];
                $affected = 0;
                $statement = $this->createMock(PDOStatement::class);
                $statement->method('bindValue')->willReturnCallback(
                    static function (string|int $key, mixed $value) use (&$bound): bool {
                        $bound[(string)$key] = $value;
                        return true;
                    }
                );
                $statement->method('execute')->willReturnCallback(
                    static function () use ($server, $sql, $ignored, $failing, &$bound, &$affected): bool {
                        $tags = [];
                        foreach ($bound as $key => $value) {
                            if (preg_match('/^:row_\d+_tag$/', $key) === 1) {
                                $tags[] = $value;
                            }
                        }
                        $server->sql[] = $sql;
                        $server->log[] = 'EXECUTE ' . implode(',', $tags);
                        foreach ($tags as $tag) {
                            if (in_array($tag, $failing, true)) {
                                throw new PDOException("simulated failure for {$tag}");
                            }
                        }
                        $firstId = 0;
                        foreach ($tags as $tag) {
                            if (in_array($tag, $ignored, true)) {
                                continue;
                            }
                            $id = ++$server->autoIncrement;
                            $server->table[$id] = $tag;
                            $firstId = $firstId ?: $id;
                            $affected++;
                        }
                        // MySQL OK packet: first generated ID of this statement, 0 if it inserted nothing.
                        $server->insertId = (string)$firstId;
                        return true;
                    }
                );
                $statement->method('rowCount')->willReturnCallback(static function () use (&$affected): int {
                    return $affected;
                });
                return $statement;
            });

            return [$pdo, $server];
        }
    }
}

