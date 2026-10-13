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
