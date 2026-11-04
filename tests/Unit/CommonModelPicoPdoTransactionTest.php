<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests;

use InvalidArgumentException;
use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use Lodur\PicoPdo\CommonModelPicoPdoUtils;
use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use RuntimeException;

/** Transaction units without a database: the PDO protocol is asserted call by call on a mocked connection. */
class CommonModelPicoPdoTransactionTest extends TestCase
{
    public function testOwnsTheTransactionWhenNoneIsOpen(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(false);
        $pdo->expects($this->once())->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('commit')->willReturn(true);
        $pdo->expects($this->never())->method('rollBack');
        $pdo->expects($this->never())->method('exec');

        $this->assertSame(42, CommonModelPicoPdoUtils::transaction($pdo, static fn(): int => 42));
    }

    public function testUsesASavepointInsideTheCallersTransaction(): void
    {
        $statements = [];
        $pdo = $this->savepointConnection($statements);
        $pdo->expects($this->never())->method('beginTransaction');
        $pdo->expects($this->never())->method('commit');
        $pdo->expects($this->never())->method('rollBack');

        $this->assertSame('done', CommonModelPicoPdoUtils::transaction($pdo, static fn(): string => 'done'));
        $this->assertCount(2, $statements);
        $this->assertMatchesRegularExpression('/^SAVEPOINT rr_[0-9a-f]{16}_\d+$/', $statements[0]);
        $this->assertSame('RELEASE ' . $statements[0], $statements[1]);
    }

    public function testEveryNestedUnitGetsItsOwnSavepoint(): void
    {
        $statements = [];
        $pdo = $this->savepointConnection($statements);

        CommonModelPicoPdoUtils::transaction($pdo, static fn(): int => 1);
        CommonModelPicoPdoUtils::transaction($pdo, static fn(): int => 2);

        $this->assertCount(4, $statements);
        $this->assertNotSame($statements[0], $statements[2]);
        $this->assertSame(substr($statements[0], 0, 30), substr($statements[2], 0, 30), 'one prefix per process');
    }

    public function testARejectedResultIsRolledBackAndStillReturned(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturnOnConsecutiveCalls(false, true);
        $pdo->expects($this->once())->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('rollBack')->willReturn(true);
        $pdo->expects($this->never())->method('commit');

        $result = CommonModelPicoPdoUtils::transaction(
            $pdo,
            static fn(): array => ['status' => false, 'error' => 'rejected'],
            static fn(array $result): bool => !$result['status']
        );

        $this->assertSame(['status' => false, 'error' => 'rejected'], $result);
    }

    public function testARejectedNestedUnitRollsBackToItsSavepointAndReleasesIt(): void
    {
        $statements = [];
        $pdo = $this->savepointConnection($statements);
        $pdo->expects($this->never())->method('rollBack');

        $isSaved = CommonModelPicoPdoUtils::transaction($pdo, static fn(): bool => false, static fn(bool $isSaved): bool => !$isSaved);

        $this->assertFalse($isSaved);
        $savepoint = substr($statements[0], strlen('SAVEPOINT '));
        $this->assertSame(
            ["SAVEPOINT {$savepoint}", "ROLLBACK TO SAVEPOINT {$savepoint}", "RELEASE SAVEPOINT {$savepoint}"],
            $statements
        );
    }

    public function testAFailingUnitIsRolledBackAndItsExceptionPropagates(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturnOnConsecutiveCalls(false, true);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('rollBack')->willReturn(true);
        $pdo->expects($this->never())->method('commit');
        $failure = new PDOException('statement failed');

        try {
            CommonModelPicoPdoUtils::transaction($pdo, static function () use ($failure): never {
                throw $failure;
            });
            $this->fail('The unit failure must propagate');
        } catch (PDOException $caught) {
            $this->assertSame($failure, $caught);
        }
    }

    public function testAFailedRollbackWrapsTheUnitFailure(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturnOnConsecutiveCalls(false, true);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('rollBack')->willReturn(false);
        $failure = new PDOException('statement failed');

        try {
            CommonModelPicoPdoUtils::transaction($pdo, static function () use ($failure): never {
                throw $failure;
            });
            $this->fail('A failed rollback must propagate');
        } catch (RuntimeException $caught) {
            $this->assertSame('Transaction unit could not be rolled back', $caught->getMessage());
            $this->assertSame($failure, $caught->getPrevious());
        }
    }

    public function testAFailedCommitIsRolledBackAndReported(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturnOnConsecutiveCalls(false, true);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('commit')->willReturn(false);
        $pdo->expects($this->once())->method('rollBack')->willReturn(true);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Transaction unit could not be committed');
        CommonModelPicoPdoUtils::transaction($pdo, static fn(): int => 1);
    }

    public function testATransactionThatCannotBeOpenedRunsNothing(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(false);
        $pdo->method('beginTransaction')->willReturn(false);
        $ran = false;

        try {
            CommonModelPicoPdoUtils::transaction($pdo, static function () use (&$ran): int {
                $ran = true;
                return 1;
            });
            $this->fail('An unopened unit must not run');
        } catch (RuntimeException $caught) {
            $this->assertSame('Transaction unit could not be opened', $caught->getMessage());
        }
        $this->assertFalse($ran);
    }

    public function testASavepointThatCannotBeSetRunsNothing(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('exec')->willReturn(false);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Transaction unit could not be opened');
        CommonModelPicoPdoUtils::transaction($pdo, static fn(): int => 1);
    }

    public function testSavepointNamesAreValidatedBeforeInterpolation(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->never())->method('exec');
        $executeSavepoint = new ReflectionMethod(CommonModelPicoPdoUtils::class, 'executeSavepoint');

        $this->expectException(InvalidArgumentException::class);
        $executeSavepoint->invoke(null, $pdo, 'SAVEPOINT', 'bad name; DROP TABLE users');
    }

    public function testTheTraitRunsUnitsOnItsOwnConnection(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(false);
        $pdo->expects($this->once())->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('commit')->willReturn(true);
        $host = $this->host($pdo);

        $this->assertFalse($host->isInTransaction());
        $this->assertSame(['status' => true], $host->runInTransaction(static fn(): array => ['status' => true]));
    }

    public function testTheTraitReportsTheConnectionsTransactionState(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(true);
        $statements = [];
        $pdo->method('exec')->willReturnCallback(static function (string $sql) use (&$statements): int {
            $statements[] = $sql;
            return 0;
        });
        $host = $this->host($pdo);

        $this->assertTrue($host->isInTransaction());
        $this->assertSame(7, $host->runInTransaction(static fn(): int => 7));
        $this->assertCount(2, $statements, 'a nested trait unit uses a savepoint');
    }

    public function testExplicitTransactionWrapsASingleBusinessWrite(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(false);
        $pdo->expects($this->once())->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('commit')->willReturn(true);
        $pdo->expects($this->never())->method('rollBack');

        $this->assertSame('ok', $this->exposedHost($pdo)->runExplicit(static fn(): string => 'ok'));
    }

    public function testExplicitTransactionRollsBackAnOwnedTransactionWhenLaterValidationThrows(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(false);
        $pdo->expects($this->once())->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('rollBack')->willReturn(true);
        $pdo->expects($this->never())->method('commit');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('rejected after the write');
        $this->exposedHost($pdo)->runExplicit(static function (): never {
            throw new RuntimeException('rejected after the write');
        });
    }

    public function testExplicitTransactionLeavesACallerOwnedTransactionAlone(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(true);
        $pdo->expects($this->never())->method('beginTransaction');
        $pdo->expects($this->never())->method('commit');
        $pdo->expects($this->never())->method('rollBack');
        $host = $this->exposedHost($pdo);

        $this->assertSame(5, $host->runExplicit(static fn(): int => 5));

        $this->expectException(RuntimeException::class);
        $host->runExplicit(static function (): never {
            throw new RuntimeException('caller decides the rollback');
        });
    }

    public function testCountGatedHelperSkipsTheTransactionForOneStatement(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->never())->method('inTransaction');
        $pdo->expects($this->never())->method('beginTransaction');
        $pdo->expects($this->never())->method('commit');

        $this->assertSame('one', $this->exposedHost($pdo)->runCounted(1, static fn(): string => 'one'));
    }

    public function testCountGatedHelperOpensOneOwnedTransactionForSeveralStatements(): void
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(false);
        $pdo->expects($this->once())->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('commit')->willReturn(true);

        $this->assertSame('many', $this->exposedHost($pdo)->runCounted(2, static fn(): string => 'many'));
    }

    public function testCountGatedHelperRollsBackOnFailureAndKeepsTheCallersTransaction(): void
    {
        $owned = $this->createMock(PDO::class);
        $owned->method('inTransaction')->willReturn(false);
        $owned->method('beginTransaction')->willReturn(true);
        $owned->expects($this->once())->method('rollBack')->willReturn(true);
        $owned->expects($this->never())->method('commit');

        try {
            $this->exposedHost($owned)->runCounted(3, static function (): never {
                throw new PDOException('chunk failed');
            });
            $this->fail('The chunk failure must propagate');
        } catch (PDOException $caught) {
            $this->assertSame('chunk failed', $caught->getMessage());
        }

        $caller = $this->createMock(PDO::class);
        $caller->method('inTransaction')->willReturn(true);
        $caller->expects($this->never())->method('beginTransaction');
        $caller->expects($this->never())->method('rollBack');
        $this->assertSame(9, $this->exposedHost($caller)->runCounted(3, static fn(): int => 9));
    }

    /**
     * A connection inside a caller's transaction; every `exec()` is recorded in `$statements`.
     *
     * @param list<string> $statements
     */
    private function savepointConnection(array &$statements): PDO
    {
        $pdo = $this->createMock(PDO::class);
        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('exec')->willReturnCallback(static function (string $sql) use (&$statements): int {
            $statements[] = $sql;
            return 0;
        });

        return $pdo;
    }

    /** Exposes the private explicit-transaction and count-gated helpers for protocol tests. */
    private function exposedHost(PDO $pdo): object
    {
        return new class ($pdo) {
            use CommonModelPicoPdoTrait;

            public function __construct(private PDO $connection)
            {
            }

            public function runExplicit(callable $work): mixed
            {
                return $this->withTransaction($work);
            }

            public function runCounted(int $statementCount, callable $work): mixed
            {
                return $this->withAtomicStatements($statementCount, $work);
            }
        };
    }

    private function host(PDO $pdo): object
    {
        return new class ($pdo) {
            use CommonModelPicoPdoTrait;

            public function __construct(private PDO $connection)
            {
            }
        };
    }
}
