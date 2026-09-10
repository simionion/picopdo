<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests;

use InvalidArgumentException;
use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Exercises connection failures and one-shot diagnostics without a database. */
class CommonModelPicoPdoDiagnosticsTest extends TestCase
{
    private const EMPTY_RESULT = ['explain' => [], 'warnings' => []];

    #[DataProvider('diagnosticFailures')]
    public function testOptionalDiagnosticFailureDoesNotPreventTheRealStatement(string $stage, bool $throws): void
    {
        $explainRows = [['id' => 1, 'select_type' => 'SIMPLE']];
        $diagnostic = $this->createMock(PDOStatement::class);
        if ($stage === 'execute') {
            if ($throws) {
                $diagnostic->method('execute')->willThrowException(new PDOException('EXPLAIN execution failed'));
            } else {
                $diagnostic->method('execute')->willReturn(false);
            }
            $diagnostic->expects($this->never())->method('fetchAll');
        } else {
            $diagnostic->method('execute')->willReturn(true);
            $diagnostic->method('fetchAll')->willReturn($explainRows);
        }
        $actual = $this->successfulStatement();
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->exactly(2))->method('prepare')
            ->willReturnCallback(static function (string $sql) use ($stage, $throws, $diagnostic, $actual): PDOStatement|false {
                if ($sql === 'EXPLAIN EXTENDED SELECT 1') {
                    if ($stage === 'prepare') {
                        if ($throws) {
                            throw new PDOException('EXPLAIN preparation failed');
                        }
                        return false;
                    }
                    return $diagnostic;
                }
                TestCase::assertSame('SELECT 1', $sql);
                return $actual;
            });
        if ($stage === 'warnings') {
            $query = $pdo->expects($this->once())->method('query')->with('SHOW WARNINGS');
            if ($throws) {
                $query->willThrowException(new PDOException('SHOW WARNINGS failed'));
            } else {
                $query->willReturn(false);
            }
        } else {
            $pdo->expects($this->never())->method('query');
        }

        $host = $this->host($pdo);
        $results = [];
        $host->debugNextStatement(static function (array $result) use (&$results): void {
            $results[] = $result;
        });
        $this->assertSame($actual, $host->prepExec('SELECT 1'));
        $expected = $stage === 'warnings' && !$throws
            ? ['explain' => $explainRows, 'warnings' => []]
            : self::EMPTY_RESULT;
        $this->assertSame([$expected], $results);
    }

    public static function diagnosticFailures(): iterable
    {
        foreach (['prepare', 'execute', 'warnings'] as $stage) {
            yield "$stage returns false" => [$stage, false];
            yield "$stage throws PDOException" => [$stage, true];
        }
    }

    #[DataProvider('statementFailures')]
    public function testStatementFailuresPropagateAsPdoExceptions(string $stage, bool $throws): void
    {
        $original = new PDOException('SQLSTATE[42000]: deliberate SQL failure');
        $pdo = $this->createMock(PDO::class);
        if ($stage === 'prepare') {
            $prepare = $pdo->expects($this->once())->method('prepare');
            if ($throws) {
                $prepare->willThrowException($original);
            } else {
                $prepare->willReturn(false);
                $pdo->method('errorInfo')->willReturn(['42000', 1064, 'deliberate SQL failure']);
            }
        } else {
            $statement = $this->createMock(PDOStatement::class);
            $pdo->method('prepare')->willReturn($statement);
            if ($throws) {
                $statement->method('execute')->willThrowException($original);
            } else {
                $statement->method('execute')->willReturn(false);
                $statement->method('errorInfo')->willReturn(['42000', 1064, 'deliberate SQL failure']);
            }
        }

        try {
            $this->host($pdo)->prepExec('INVALID SQL');
            $this->fail('The real SQL failure must not be suppressed');
        } catch (PDOException $error) {
            $this->assertStringContainsString('42000', $error->getMessage());
            $this->assertStringContainsString('deliberate SQL failure', $error->getMessage());
            if ($throws) {
                $this->assertSame($original, $error, 'Preserve the driver exception and its diagnostic information');
            }
        }
    }

    public static function statementFailures(): iterable
    {
        foreach (['prepare', 'execute'] as $stage) {
            yield "$stage returns false" => [$stage, false];
            yield "$stage throws PDOException" => [$stage, true];
        }
    }

    public function testDebugCallbackCanBeReplacedAndIsConsumedBeforeAReentrantStatement(): void
    {
        $statement = $this->successfulStatement();
        $pdo = $this->createMock(PDO::class);
        $sqlLog = [];
        $pdo->method('prepare')->willReturnCallback(static function (string $sql) use (&$sqlLog, $statement): PDOStatement|false {
            $sqlLog[] = $sql;
            return str_starts_with($sql, 'EXPLAIN ') ? false : $statement;
        });
        $host = $this->host($pdo);
        $calls = [];
        $host->debugNextStatement(static function () use (&$calls): void {
            $calls[] = 'replaced';
        });
        $host->debugNextStatement(static function (array $result) use (&$calls, $host): void {
            $calls[] = $result;
            $host->prepExec('SELECT 2');
        });

        $host->prepExec('SELECT 1');
        $host->prepExec('SELECT 3');

        $this->assertSame([self::EMPTY_RESULT], $calls);
        $this->assertSame(['EXPLAIN EXTENDED SELECT 1', 'SELECT 2', 'SELECT 1', 'SELECT 3'], $sqlLog);
    }

    public function testDebugCallbackCanArmTheFollowingStatement(): void
    {
        $statement = $this->successfulStatement();
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->exactly(5))->method('prepare')
            ->willReturnCallback(static fn (string $sql): PDOStatement|false => str_starts_with($sql, 'EXPLAIN ') ? false : $statement);
        $host = $this->host($pdo);
        $calls = [];
        $host->debugNextStatement(static function () use (&$calls, $host): void {
            $calls[] = 'first';
            $host->debugNextStatement(static function () use (&$calls): void {
                $calls[] = 'second';
            });
        });
        $host->prepExec('SELECT 1');
        $this->assertSame(['first'], $calls);
        $host->prepExec('SELECT 2');
        $host->prepExec('SELECT 3');
        $this->assertSame(['first', 'second'], $calls);
    }

    public function testEmptyWritesNeedNoConnectionAndPreserveThePendingDebugCallback(): void
    {
        $host = $this->host();
        $calls = [];
        $host->debugNextStatement(static function (array $result) use (&$calls): void {
            $calls[] = $result;
        });

        // No payload means other arguments are irrelevant, including invalid table/WHERE input.
        $this->assertSame(0, $host->update('invalid table!', [], null));
        $this->assertSame(0, $host->update('invalid table!', [], [], ['unused' => [1]], 'INVALID TAIL'));
        $this->assertSame(0, $host->delete('invalid table!', [], ['unused' => [1]], 'INVALID TAIL'));
        $this->assertSame([], $calls);
        $this->assertSame(0, $host->connectionRequests);

        $statement = $this->successfulStatement();
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->exactly(2))->method('prepare')->willReturn(false, $statement);
        $host->connect($pdo);
        $this->assertSame($statement, $host->prepExec('SELECT 1'));
        $this->assertSame([self::EMPTY_RESULT], $calls);
    }

    #[DataProvider('invalidNonemptyWrites')]
    public function testNonemptyWritesStillRejectMalformedInputBeforeConnecting(string $method, array $arguments): void
    {
        $host = $this->host();
        try {
            $host->$method(...$arguments);
            $this->fail('Nonempty malformed writes must still be rejected');
        } catch (InvalidArgumentException) {
            $this->assertSame(0, $host->connectionRequests);
        }
    }

    public static function invalidNonemptyWrites(): iterable
    {
        yield 'UPDATE without WHERE' => ['update', ['users', ['name' => 'A']]];
        yield 'UPDATE empty WHERE' => ['update', ['users', ['name' => 'A'], []]];
        yield 'UPDATE blank WHERE' => ['update', ['users', ['name' => 'A'], '   ']];
        yield 'UPDATE empty batch row' => ['update', ['users', [[]], [['id' => 1]]]];
        yield 'UPDATE empty batch condition' => ['update', ['users', [['name' => 'A']], [[]]]];
        yield 'DELETE blank WHERE' => ['delete', ['users', '   ']];
        yield 'DELETE empty batch condition' => ['delete', ['users', [[]]]];
        yield 'UPDATE empty IN' => ['update', ['users', ['name' => 'A'], ['id' => []]]];
        yield 'DELETE empty IN' => ['delete', ['users', ['id' => []]]];
    }

    public function testInvalidInBindingsDoNotConsumePendingDebugCallback(): void
    {
        $host = $this->host();
        $calls = [];
        $host->debugNextStatement(static function (array $result) use (&$calls): void {
            $calls[] = $result;
        });
        try {
            $host->prepExec('SELECT id FROM users WHERE id IN (:ids)', ['ids' => []]);
            $this->fail('Empty IN values must be rejected');
        } catch (InvalidArgumentException) {
            $this->assertSame([], $calls);
            $this->assertSame(0, $host->connectionRequests);
        }
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->exactly(2))->method('prepare')->willReturn(false, $this->successfulStatement());
        $host->connect($pdo);
        $host->prepExec('SELECT 1');
        $this->assertSame([self::EMPTY_RESULT], $calls);
    }

    private function successfulStatement(): PDOStatement
    {
        $statement = $this->createMock(PDOStatement::class);
        $statement->method('execute')->willReturn(true);
        return $statement;
    }

    private function host(?PDO $pdo = null): object
    {
        return new class ($pdo) {
            use CommonModelPicoPdoTrait {
                debugNextStatement as public;
                prepExec as public;
                update as public;
                delete as public;
                pdo as private traitPdo;
            }

            public int $connectionRequests = 0;

            public function __construct(?PDO $pdo)
            {
                if ($pdo !== null) {
                    $this->connect($pdo);
                }
            }

            public function connect(PDO $pdo): void
            {
                $this->pdo = $pdo;
            }

            protected function pdo(): PDO
            {
                ++$this->connectionRequests;
                return $this->traitPdo();
            }
        };
    }
}
