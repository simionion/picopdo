<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests\Integration;

use InvalidArgumentException;
use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Composite SELECT conditions preserve membership pairs on a real database connection. */
final class CommonModelPicoPdoRowSetSelectTest extends TestCase
{
    private const TABLE = 'test_row_set_select_upstream';

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
            id INT PRIMARY KEY,
            org_id INT NOT NULL,
            person_id INT NOT NULL,
            label VARCHAR(64) NOT NULL,
            note VARCHAR(64) NULL,
            UNIQUE KEY membership (org_id, person_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
        $this->pdo->exec("INSERT INTO " . self::TABLE . " VALUES
            (1, 10, 100, 'A', NULL),
            (2, 20, 200, 'B', 'n'),
            (3, 10, 200, 'cross-a', NULL),
            (4, 20, 100, 'cross-b', NULL),
            (5, 30, 300, 'C', 'n')");
        $this->model = new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                select as public;
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
    public function testExactPairsExcludeCrossedMemberships(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            ['org_id' => 10, 'person_id' => 100],
            ['org_id' => 20, 'person_id' => 200],
        ], null, 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'B']], $rows);
    }

    #[DataProvider('prepareModes')]
    public function testSelectReturnsAStatementAndHonoursTheTail(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $statement = $this->model->select(self::TABLE, ['label'], [
            ['org_id' => 10, 'person_id' => 100],
            ['org_id' => 20, 'person_id' => 200],
        ], null, 'ORDER BY id DESC LIMIT 1');

        $this->assertInstanceOf(PDOStatement::class, $statement);
        $this->assertSame(['B'], $statement->fetchAll(PDO::FETCH_COLUMN));
    }

    #[DataProvider('prepareModes')]
    public function testSelectOneUsesTheRowSetAndReturnsAnEmptyArrayForNoMatch(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $this->assertSame(['label' => 'B'], $this->model->selectOne(self::TABLE, 'label', [
            ['org_id' => 10, 'person_id' => 100],
            ['org_id' => 20, 'person_id' => 200],
        ], null, 'ORDER BY id DESC'));
        $this->assertSame([], $this->model->selectOne(self::TABLE, 'label', [
            ['org_id' => 99, 'person_id' => 999],
        ]));
    }

    #[DataProvider('prepareModes')]
    public function testDuplicateAndReversedPairsKeepTheSameRows(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            ['org_id' => 20, 'person_id' => 200],
            ['org_id' => 10, 'person_id' => 100],
            ['org_id' => 20, 'person_id' => 200],
        ], null, 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'B']], $rows);
    }

    #[DataProvider('prepareModes')]
    public function testSparseConditionKeysRemainCompositePairs(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            8 => ['org_id' => 10, 'person_id' => 100],
            31 => ['org_id' => 20, 'person_id' => 200],
        ], null, 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'B']], $rows);
    }

    #[DataProvider('prepareModes')]
    public function testPerRowNamedBindingsAreIsolatedAndSparseListsAlignByPosition(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            9 => ['org_id = :org', 'person_id = :person'],
            70 => ['org_id = :org', 'person_id = :person'],
        ], [
            4 => [':org' => 10, ':person' => 100],
            88 => [':org' => 20, ':person' => 200],
        ], 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'B']], $rows);
    }

    #[DataProvider('prepareModes')]
    public function testACommonNamedBindingsMapCanBeSharedAcrossGroups(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            ['org_id = :first_org AND person_id = :first'],
            ['org_id = :second_org AND person_id = :second'],
        ], [':first_org' => 10, ':first' => 100, ':second_org' => 10, ':second' => 200], 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'cross-a']], $rows);
    }

    public function testEmulatedPreparesCanReuseTheSameNamedBindingAcrossGroups(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            ['org_id = :org AND person_id = :first'],
            ['org_id = :org AND person_id = :second'],
        ], [':org' => 10, ':first' => 100, ':second' => 200], 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'cross-a']], $rows);
    }

    #[DataProvider('prepareModes')]
    public function testPerRowPositionalBindingsRemainInTheirOwnGroups(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            ['org_id = ? AND person_id = ?'],
            ['org_id = ? AND person_id = ?'],
        ], [[10, 100], [20, 200]], 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'B']], $rows);
    }

    #[DataProvider('prepareModes')]
    public function testDirectQuestionMarkKeysBindValuesWithinEachPair(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            ['org_id = ?' => 10, 'person_id = ?' => 100],
            ['org_id = ?' => 20, 'person_id = ?' => 200],
        ], null, 'ORDER BY id');

        $this->assertSame([['label' => 'A'], ['label' => 'B']], $rows);
    }

    #[DataProvider('prepareModes')]
    public function testExplicitNullAndInPredicatesStayInsideTheirConditionGroups(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $this->assertSame([['label' => 'A'], ['label' => 'cross-a'], ['label' => 'C']],
            $this->model->selectAll(self::TABLE, 'label', [
                ['org_id' => 10, 'note IS NULL'],
                ['org_id' => 30, 'note' => 'n'],
            ], null, 'ORDER BY id'));
        $this->assertSame([['label' => 'A'], ['label' => 'B'], ['label' => 'cross-a']],
            $this->model->selectAll(self::TABLE, 'label', [
                ['org_id' => 10, 'person_id IN (?)' => [100, 200]],
                ['org_id' => 20, 'person_id' => 200],
            ], null, 'ORDER BY id'));
    }

    #[DataProvider('prepareModes')]
    public function testForUpdateTailKeepsTheCallerTransactionOpen(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $this->pdo->beginTransaction();
        $rows = $this->model->selectAll(self::TABLE, 'label', [
            ['org_id' => 10, 'person_id' => 100],
            ['org_id' => 20, 'person_id' => 200],
        ], null, 'ORDER BY id FOR UPDATE');

        $this->assertSame([['label' => 'A'], ['label' => 'B']], $rows);
        $this->assertTrue($this->pdo->inTransaction());
        $this->pdo->rollBack();
        $this->assertFalse($this->pdo->inTransaction());
    }

    #[DataProvider('prepareModes')]
    public function testFlatRawNullAndEmptyFiltersKeepTheirExistingMeaning(bool $emulated): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $this->assertSame([['label' => 'A']], $this->model->selectAll(self::TABLE, 'label', ['org_id' => 10, 'person_id' => 100]));
        $this->assertSame([['label' => 'B']], $this->model->selectAll(self::TABLE, 'label', 'org_id = ? AND person_id = ?', [20, 200]));
        $this->assertSame([['label' => 'A']], $this->model->selectAll(self::TABLE, 'label', ['org_id = 10', 'person_id = 100']));
        $this->assertCount(5, $this->model->selectAll(self::TABLE, 'label', null));
        $this->assertCount(5, $this->model->selectAll(self::TABLE, 'label', []));
        $this->assertSame([], $this->model->selectAll(self::TABLE, 'label', ['note' => null]));
        $this->assertCount(4, $this->model->selectAll(self::TABLE, 'label', ['org_id IN (?)' => [10, 20], 'person_id IN (?)' => [100, 200]]));
    }

    public static function emptyGroups(): array
    {
        $groups = [
            'only empty group' => [[]],
            'leading empty group' => [[], ['org_id' => 10]],
            'trailing empty group' => [['org_id' => 10], []],
            'all empty groups' => [[], []],
        ];
        $cases = [];
        foreach (self::prepareModes() as $mode => [$emulated]) {
            foreach ($groups as $name => $where) {
                $cases[$mode . ': ' . $name] = [$emulated, $where];
            }
        }

        return $cases;
    }

    #[DataProvider('emptyGroups')]
    public function testAnEmptyConditionGroupIsRejected(bool $emulated, array $where): void
    {
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $emulated);
        $this->expectException(InvalidArgumentException::class);
        $this->model->selectAll(self::TABLE, 'label', $where);
    }
}
