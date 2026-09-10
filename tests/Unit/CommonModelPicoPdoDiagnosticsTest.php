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
    public function testDiagnosticFailuresPropagateBeforeTheRealStatementAndConsumeTheCallback(
        string $failedSql,
        string $stage,
        bool $throws
    ): void {
        $original = new PDOException('Diagnostic ' . $stage . ' failed');
        $statements = [];
        foreach (['EXPLAIN EXTENDED SELECT 1', 'SHOW WARNINGS'] as $sql) {
            $statement = $this->createMock(PDOStatement::class);
            if ($sql === $failedSql && $stage === 'execute') {
                $execute = $statement->expects($this->once())->method('execute');
                if ($throws) {
                    $execute->willThrowException($original);
                } else {
                    $execute->willReturn(false);
                    $statement->method('errorInfo')->willReturn(['HY000', 1, 'Diagnostic execution failed']);
                }
                $statement->expects($this->never())->method('fetchAll');
            } else {
                $statement->method('execute')->willReturn(true);
                $statement->method('fetchAll')->willReturn([['id' => 1]]);
            }
            $statements[$sql] = $statement;
        }
        $actual = $this->successfulStatement();
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->never())->method('query');
        $pdo->expects($this->never())->method('setAttribute');
        $pdo->method('errorInfo')->willReturn(['HY000', 1, 'Diagnostic preparation failed']);
        $prepared = [];
        $pdo->method('prepare')->willReturnCallback(
            static function (string $sql) use ($failedSql, $stage, $throws, $statements, $actual, $original, &$prepared): PDOStatement|false {
                $prepared[] = $sql;
                if ($sql === $failedSql && $stage === 'prepare') {
                    if ($throws) {
                        throw $original;
                    }
                    return false;
                }
                return in_array($sql, ['SELECT 1', 'SELECT 2'], true) ? $actual : $statements[$sql];
            }
        );

        $host = $this->host($pdo);
        $results = [];
        $host->debugNextStatement(static function (array $result) use (&$results): void {
            $results[] = $result;
        });
        try {
            $host->prepExec('SELECT 1');
            $this->fail('Diagnostic failures must propagate before the real statement runs');
        } catch (PDOException $error) {
            $this->assertStringContainsString('Diagnostic', $error->getMessage());
            if ($throws) {
                $this->assertSame($original, $error);
            }
        }
        $this->assertSame([], $results, 'A failed diagnostic must not invoke the callback');
        $this->assertSame(
            $failedSql === 'SHOW WARNINGS'
                ? ['EXPLAIN EXTENDED SELECT 1', 'SHOW WARNINGS']
                : ['EXPLAIN EXTENDED SELECT 1'],
            $prepared
        );
        $this->assertSame($actual, $host->prepExec('SELECT 2'));
        $this->assertSame('SELECT 2', $prepared[array_key_last($prepared)]);
        $this->assertSame([], $results, 'A failed diagnostic still consumes its one-shot callback');
    }

    public static function diagnosticFailures(): iterable
    {
        foreach (['EXPLAIN EXTENDED SELECT 1', 'SHOW WARNINGS'] as $sql) {
            foreach (['prepare', 'execute'] as $stage) {
                yield "$sql $stage returns false" => [$sql, $stage, false];
                yield "$sql $stage throws PDOException" => [$sql, $stage, true];
            }
        }
    }

    public function testDiagnosticsUsePrepExecWithTheSameExpandedTypedBindingsAsTheRealQuery(): void
    {
        $sql = 'SELECT id FROM users WHERE id IN (:ids) AND active = :active'
            . ' AND deleted_at <=> :deleted AND name = :name LIMIT ?';
        $params = ['ids' => [7, 11], 'active' => true, 'deleted' => null, 'name' => 'A', 2];
        $expandedSql = 'SELECT id FROM users WHERE id IN (:ids0,:ids1) AND active = :active'
            . ' AND deleted_at <=> :deleted AND name = :name LIMIT :nph_0';
        $explain = $this->successfulStatement();
        $actual = $this->successfulStatement();
        $warnings = $this->successfulStatement();
        $plan = [['id' => 1, 'table' => 'users']];
        $warningRows = [['Level' => 'Note', 'Code' => 1003, 'Message' => 'rewritten query']];
        $explain->expects($this->once())->method('fetchAll')->with(PDO::FETCH_ASSOC)->willReturn($plan);
        $warnings->expects($this->once())->method('fetchAll')->with(PDO::FETCH_ASSOC)->willReturn($warningRows);
        $warnings->expects($this->never())->method('bindValue');
        $bindings = [];
        foreach (['explain' => $explain, 'actual' => $actual] as $name => $statement) {
            $statement->expects($this->exactly(6))->method('bindValue')->willReturnCallback(
                static function (string $key, mixed $value, int $type) use ($name, &$bindings): bool {
                    $bindings[$name][$key] = [$value, $type];
                    return true;
                }
            );
        }
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->never())->method('query');
        $prepared = [];
        $pdo->expects($this->exactly(3))->method('prepare')->willReturnCallback(
            static function (string $query) use ($expandedSql, $explain, $warnings, $actual, &$prepared): PDOStatement {
                $prepared[] = $query;
                return match ($query) {
                    "EXPLAIN EXTENDED {$expandedSql}" => $explain,
                    'SHOW WARNINGS' => $warnings,
                    $expandedSql => $actual,
                };
            }
        );
        $host = $this->host($pdo);
        $results = [];
        $host->debugNextStatement(static function (array $result) use (&$results): void {
            $results[] = $result;
        });

        $this->assertSame($actual, $host->prepExec($sql, $params));
        $this->assertSame([['explain' => $plan, 'warnings' => $warningRows]], $results);
        $this->assertSame(["EXPLAIN EXTENDED {$expandedSql}", 'SHOW WARNINGS', $expandedSql], $prepared);
        $this->assertSame([$sql, "EXPLAIN EXTENDED {$expandedSql}", 'SHOW WARNINGS'], $host->executionCalls);
        $expectedBindings = [
            ':ids0' => [7, PDO::PARAM_INT],
            ':ids1' => [11, PDO::PARAM_INT],
            ':active' => [true, PDO::PARAM_BOOL],
            ':deleted' => [null, PDO::PARAM_NULL],
            ':name' => ['A', PDO::PARAM_STR],
            ':nph_0' => [2, PDO::PARAM_INT],
        ];
        $this->assertSame($expectedBindings, $bindings['explain']);
        $this->assertSame($expectedBindings, $bindings['actual']);
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

    #[DataProvider('statementFailures')]
    public function testRealStatementFailuresPropagateAfterSuccessfulDiagnostics(string $stage, bool $throws): void
    {
        $original = new PDOException('Real statement failed');
        $diagnostic = $this->successfulStatement();
        $diagnostic->method('fetchAll')->willReturn([]);
        $actual = $this->createMock(PDOStatement::class);
        if ($stage === 'execute') {
            $execute = $actual->expects($this->once())->method('execute');
            if ($throws) {
                $execute->willThrowException($original);
            } else {
                $execute->willReturn(false);
                $actual->method('errorInfo')->willReturn(['HY000', 1, 'Real statement failed']);
            }
        }
        $pdo = $this->createMock(PDO::class);
        $pdo->method('errorInfo')->willReturn(['HY000', 1, 'Real statement failed']);
        $prepared = [];
        $pdo->expects($this->exactly(3))->method('prepare')->willReturnCallback(
            static function (string $sql) use ($stage, $throws, $original, $diagnostic, $actual, &$prepared): PDOStatement|false {
                $prepared[] = $sql;
                if ($sql !== 'SELECT 1') {
                    return $diagnostic;
                }
                if ($stage === 'prepare') {
                    if ($throws) {
                        throw $original;
                    }
                    return false;
                }
                return $actual;
            }
        );
        $host = $this->host($pdo);
        $results = [];
        $host->debugNextStatement(static function (array $result) use (&$results): void {
            $results[] = $result;
        });
        try {
            $host->prepExec('SELECT 1');
            $this->fail('The real SQL failure must propagate after successful diagnostics');
        } catch (PDOException $error) {
            $this->assertStringContainsString('Real statement failed', $error->getMessage());
            if ($throws) {
                $this->assertSame($original, $error);
            }
        }
        $this->assertSame([self::EMPTY_RESULT], $results);
        $this->assertSame(['EXPLAIN EXTENDED SELECT 1', 'SHOW WARNINGS', 'SELECT 1'], $prepared);
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
            return $statement;
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
        $this->assertSame(['EXPLAIN EXTENDED SELECT 1', 'SHOW WARNINGS', 'SELECT 2', 'SELECT 1', 'SELECT 3'], $sqlLog);
    }

    public function testDebugCallbackCanArmTheFollowingStatement(): void
    {
        $statement = $this->successfulStatement();
        $pdo = $this->createMock(PDO::class);
        $pdo->expects($this->exactly(7))->method('prepare')->willReturn($statement);
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

    public function testDebugCallbackPdoExceptionPropagatesAndTheCallbackRemainsConsumed(): void
    {
        $statement = $this->successfulStatement();
        $statement->method('fetchAll')->willReturn([]);
        $pdo = $this->createMock(PDO::class);
        $prepared = [];
        $pdo->expects($this->exactly(3))->method('prepare')->willReturnCallback(
            static function (string $sql) use ($statement, &$prepared): PDOStatement {
                $prepared[] = $sql;
                return $statement;
            }
        );
        $host = $this->host($pdo);
        $original = new PDOException('Debug callback failure');
        $calls = 0;
        $host->debugNextStatement(static function () use ($original, &$calls): void {
            ++$calls;
            throw $original;
        });

        try {
            $host->prepExec('SELECT 1');
            $this->fail('Callback failures must propagate');
        } catch (PDOException $error) {
            $this->assertSame($original, $error);
        }
        $this->assertSame($statement, $host->prepExec('SELECT 2'));
        $this->assertSame(1, $calls);
        $this->assertSame(['EXPLAIN EXTENDED SELECT 1', 'SHOW WARNINGS', 'SELECT 2'], $prepared);
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
        $pdo->expects($this->exactly(3))->method('prepare')->willReturn($statement);
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
        $pdo->expects($this->exactly(3))->method('prepare')->willReturn($this->successfulStatement());
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
                prepExec as private traitPrepExec;
                update as public;
                delete as public;
                pdo as private traitPdo;
            }

            public int $connectionRequests = 0;
            public array $executionCalls = [];

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

            public function prepExec(string $sql, array|string|int|null $params = null): PDOStatement
            {
                $this->executionCalls[] = $sql;
                return $this->traitPrepExec($sql, $params);
            }

            protected function pdo(): PDO
            {
                ++$this->connectionRequests;
                return $this->traitPdo();
            }
        };
    }
}
