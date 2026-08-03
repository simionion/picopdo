<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use Lodur\PicoPdo\CommonModelPicoPdoUtils;
use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;

/**
 * Batch UPDATE staged through a temporary table, against MariaDB.
 *
 * At {@see CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS} rows the same payload switches from
 * CASE/WHEN to a staging table + JOIN, so every test here runs the payload **both ways** — a
 * non-empty `$sqlTail` forces the CASE/WHEN path — and asserts the two agree on the affected
 * count and on every persisted row. The paths compile completely different SQL from the same
 * input, so equivalence is the only assertion that actually pins the behaviour down; asserting
 * the JOIN SQL alone would happily pass while the WHERE means something else.
 */
class CommonModelPicoPdoTraitTempTableUpdateTest extends TestCase
{
    private const TABLE = 'unit_trait_temp_update';

    /** Row count that trips the temp-table path. */
    private const ROWS = CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS;

    private PDO $pdo;

    private object $db;

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
            'CREATE TABLE ' . self::TABLE . ' (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tenant INT NOT NULL DEFAULT 1,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(32) NOT NULL DEFAULT \'active\',
                views INT NOT NULL DEFAULT 0
            )'
        );

        $this->db = new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                insert as public;
                update as public;
                prepExec as traitPrepExec;
            }

            /** @var list<string> Every statement prepExec() ran, newest last. */
            public array $sql = [];

            public function __construct(PDO $pdo)
            {
                $this->pdo = $pdo;
            }

            protected function prepExec(string $sql, array|string|int|null $params = null): PDOStatement
            {
                $this->sql[] = $sql;

                return $this->traitPrepExec($sql, $params);
            }
        };
    }

    protected function tearDown(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        parent::tearDown();
    }

    /** A plain equal-shaped batch is staged and joined, not compiled into CASE/WHEN. */
    public function testBatchAtThresholdUsesTheTempTablePath(): void
    {
        $this->seed();
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i]);

        $this->db->update(self::TABLE, $data, $where);

        $this->assertTrue($this->usedTempTablePath(), 'Expected the staging-table JOIN path');
        $this->assertStringContainsString('JOIN', $this->joinUpdateSql());
        $this->assertStringNotContainsString('CASE', $this->joinUpdateSql());

        // One row short of the threshold the same shape stays on CASE/WHEN.
        $this->seed();
        $this->db->sql = [];
        $this->db->update(self::TABLE, array_slice($data, 0, self::ROWS - 1), array_slice($where, 0, self::ROWS - 1));
        $this->assertFalse($this->usedTempTablePath());
    }

    /**
     * A condition identical on every WHERE row is peeled off and applied once against the update
     * target, filtering which staged rows match — here only tenant 1, i.e. half the batch.
     */
    public function testCommonConditionFiltersTheJoin(): void
    {
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'tenant' => 1]);

        [$affected, $rows] = $this->runBothPaths($data, $where);

        $this->assertSame(self::ROWS / 2, $affected, 'Only tenant 1 rows may be updated');
        $this->assertStringContainsString('t.tenant = :', $this->joinUpdateSql());

        foreach ($rows as $row) {
            $id = (int)$row['id'];
            $this->assertSame(
                (int)$row['tenant'] === 1 ? "Renamed {$id}" : "User {$id}",
                $row['name'],
                "id={$id}"
            );
        }
    }

    /** Several shared conditions peel together; what varies per row stays the join key. */
    public function testMultipleCommonConditionsPeelTogether(): void
    {
        [$data, $where] = $this->payload(
            static fn (int $i): array => ['id' => $i, 'tenant' => 1, 'status' => 'active']
        );

        [$affected] = $this->runBothPaths($data, $where);

        $expected = (int)$this->pdo->query(
            'SELECT COUNT(*) FROM ' . self::TABLE . " WHERE tenant = 1 AND status = 'active'"
        )->fetchColumn();
        $this->assertSame($expected, $affected);
        $this->assertGreaterThan(0, $affected);
        $this->assertStringContainsString('t.tenant = :', $this->joinUpdateSql());
        $this->assertStringContainsString('t.status = :', $this->joinUpdateSql());
    }

    /** A single shared bindings map feeds the `:name` placeholders of the peeled clause. */
    public function testSharedBindingsMapWithNamedPlaceholder(): void
    {
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'views > :min']);

        [$affected] = $this->runBothPaths($data, $where, [':min' => 100]);

        $this->assertSame(self::ROWS - 100, $affected);
        // The user's name is isolated under tmp_c_ so it cannot collide with the join's own binds.
        $this->assertStringContainsString('t.views > :tmp_c_min', $this->joinUpdateSql());
    }

    /**
     * Two placeholders in one shared clause, plus a bind the clause never references.
     *
     * Only the fragment's leading column is qualified; `views` is not staged here, so the trailing
     * bare reference resolves to the update target on its own.
     */
    public function testSharedBindingsMapWithMultiplePlaceholders(): void
    {
        [$data, $where] = $this->payload(
            static fn (int $i): array => ['id' => $i, 'views > :min AND views <= :max']
        );

        [$affected] = $this->runBothPaths($data, $where, [':min' => 50, ':max' => 150, ':unused' => 'x']);

        $this->assertSame(100, $affected);
        $this->assertStringContainsString('t.views > :tmp_c_min AND views <= :tmp_c_max', $this->joinUpdateSql());
    }

    /**
     * Regression: a shared fragment naming a column the staging table also carries would leave
     * that reference bare — `... AND views > 100` with `views` on both sides of the JOIN — and
     * the server refused the statement with 1052 "Column 'views' in WHERE is ambiguous", at
     * exactly TEMP_UPDATE_MIN_ROWS rows only. Such a fragment must stay on CASE/WHEN.
     */
    public function testCommonConditionNamingAStagedColumnFallsBack(): void
    {
        // Trailing reference is a SET column.
        $this->seed();
        $data = $where = [];
        for ($i = 1; $i <= self::ROWS; $i++) {
            $data[] = ['views' => $i * 2];
            $where[] = ['id' => $i, 'tenant = 1 AND views > 100'];
        }
        $affected = $this->db->update(self::TABLE, $data, $where);
        $this->assertFalse($this->usedTempTablePath());
        $this->assertSame(50, $affected);

        // Trailing reference is the join key.
        $this->seed();
        [$data, $where] = $this->payload(
            static fn (int $i): array => ['id' => $i, 'tenant = 1 AND id > 100']
        );
        $affected = $this->db->update(self::TABLE, $data, $where);
        $this->assertFalse($this->usedTempTablePath());
        $this->assertSame(50, $affected);
    }

    /**
     * Regression: a table-qualified key was prefixed a second time (`t.users.tenant`) and the
     * server answered 1054 unknown column. The alias replaces the table name inside the JOIN, so
     * an already-qualified reference cannot be rewritten — it belongs on CASE/WHEN.
     */
    public function testDottedCommonConditionKeyFallsBack(): void
    {
        $this->seed();
        [$data, $where] = $this->payload(
            static fn (int $i): array => ['id' => $i, self::TABLE . '.tenant' => 1]
        );

        $affected = $this->db->update(self::TABLE, $data, $where);

        $this->assertFalse($this->usedTempTablePath());
        $this->assertSame(self::ROWS / 2, $affected);
    }

    /** An `IN (:list)` shared clause survives the array expansion prepExec() does last. */
    public function testSharedBindingsMapWithInList(): void
    {
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'status IN (:sts)']);

        [$affected] = $this->runBothPaths($data, $where, [':sts' => ['active', 'pending']]);

        $expected = (int)$this->pdo->query(
            'SELECT COUNT(*) FROM ' . self::TABLE . " WHERE status IN ('active', 'pending')"
        )->fetchColumn();
        $this->assertSame($expected, $affected);
        $this->assertGreaterThan(0, $affected);
    }

    /** A per-row list of bindings maps collapses when every row binds the same value. */
    public function testPerRowBindingsMapListWithIdenticalValues(): void
    {
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'status = :st']);

        [$affected] = $this->runBothPaths($data, $where, array_fill(0, self::ROWS, [':st' => 'active']));

        $expected = (int)$this->pdo->query(
            'SELECT COUNT(*) FROM ' . self::TABLE . " WHERE status = 'active'"
        )->fetchColumn();
        $this->assertSame($expected, $affected);
        $this->assertGreaterThan(0, $affected);
        $this->assertStringContainsString('t.status = :tmp_c_st', $this->joinUpdateSql());
    }

    /**
     * The same name bound to a different value per row is exactly what a single shared clause
     * cannot express, so the batch must go back to CASE/WHEN — where each row keeps its value.
     */
    public function testPerRowBindingsMapListWithVaryingValuesFallsBack(): void
    {
        $this->seed();
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'status = :st']);
        $bindings = array_map(
            static fn (int $i): array => [':st' => $i % 2 === 0 ? 'active' : 'pending'],
            range(1, self::ROWS)
        );

        $affected = $this->db->update(self::TABLE, $data, $where, $bindings);

        $this->assertFalse($this->usedTempTablePath(), 'Per-row values must not be flattened into one clause');

        // Each row was matched against its own :st value, not a shared one.
        foreach ($this->snapshot() as $row) {
            $id = (int)$row['id'];
            $wanted = $id % 2 === 0 ? 'active' : 'pending';
            $this->assertSame(
                $this->seedStatus($id) === $wanted ? "Renamed {$id}" : "User {$id}",
                $row['name'],
                "id={$id}"
            );
        }
        $this->assertSame(
            count(array_filter(
                range(1, self::ROWS),
                fn (int $i): bool => $this->seedStatus($i) === ($i % 2 === 0 ? 'active' : 'pending')
            )),
            $affected
        );
    }

    /**
     * Regression: a peeled raw fragment with a positional `?` draws its value from the bindings
     * list. Resolving only `:name` binds left the `?` unfed and the batch threw "Not enough
     * positional bindings for raw SQL clause placeholders" at exactly TEMP_UPDATE_MIN_ROWS rows,
     * while one row fewer went through CASE/WHEN and worked.
     */
    public function testPerRowBindingsListWithPositionalPlaceholder(): void
    {
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'views > ?']);

        [$affected] = $this->runBothPaths($data, $where, array_fill(0, self::ROWS, [100]));

        $this->assertSame(self::ROWS - 100, $affected);
        $this->assertTrue($this->usedTempTablePath());
        $this->assertStringContainsString('t.views > :tmp_c_0', $this->joinUpdateSql());
    }

    /** A keyed `col > ?` entry carries its own value, so no bindings list is involved. */
    public function testKeyedPositionalCommonCondition(): void
    {
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'views > ?' => 100]);

        [$affected] = $this->runBothPaths($data, $where);

        $this->assertSame(self::ROWS - 100, $affected);
        $this->assertTrue($this->usedTempTablePath());
    }

    /** Positional values that vary per row cannot collapse either — CASE/WHEN keeps them apart. */
    public function testPerRowPositionalBindingsWithVaryingValuesFallBack(): void
    {
        $this->seed();
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'views > ?']);
        $bindings = array_map(static fn (int $i): array => [$i], range(1, self::ROWS));

        // Row i matches only when views (= i) > i, which never holds: nothing may change.
        $affected = $this->db->update(self::TABLE, $data, $where, $bindings);

        $this->assertFalse($this->usedTempTablePath());
        $this->assertSame(0, $affected);
        $this->assertSame('User 1', $this->snapshot()[0]['name']);
    }

    /** A shared clause the JOIN cannot qualify (no leading column) stays on CASE/WHEN. */
    public function testUnqualifiableCommonConditionFallsBack(): void
    {
        $this->seed();
        [$data, $where] = $this->payload(
            static fn (int $i): array => ['id' => $i, '(views > 100 OR status = \'active\')']
        );

        $affected = $this->db->update(self::TABLE, $data, $where);

        $this->assertFalse($this->usedTempTablePath());
        $this->assertGreaterThan(0, $affected);
    }

    /** The staging table is dropped again, so a second batch on the same connection is clean. */
    public function testStagingTableDoesNotLeakBetweenBatches(): void
    {
        $this->seed();
        [$data, $where] = $this->payload(static fn (int $i): array => ['id' => $i, 'tenant' => 1]);

        $this->db->update(self::TABLE, $data, $where);
        $this->db->sql = [];
        $second = $this->db->update(self::TABLE, $data, $where);

        $this->assertTrue($this->usedTempTablePath());
        // Same values re-applied: matched but unchanged, so MySQL reports 0 changed rows.
        $this->assertSame(0, $second);
    }

    /**
     * Runs the payload through both paths over identical seed data and asserts they agree.
     *
     * @param list<array<string, mixed>> $data
     * @param list<array<string|int, mixed>> $where
     * @param int|string|array<mixed>|null $bindings
     * @return array{0: int, 1: list<array<string, mixed>>} Affected count and the final snapshot
     */
    private function runBothPaths(array $data, array $where, int|string|array|null $bindings = null): array
    {
        // A non-empty SQL tail is rejected by the plan builder, so this is the same payload on
        // CASE/WHEN; `ORDER BY id` is valid for a single-table UPDATE and changes no result.
        $this->seed();
        $this->db->sql = [];
        $caseAffected = $this->db->update(self::TABLE, $data, $where, $bindings, 'ORDER BY id');
        $caseRows = $this->snapshot();
        $this->assertFalse($this->usedTempTablePath(), 'Reference run must stay on CASE/WHEN');

        $this->seed();
        $this->db->sql = [];
        $tempAffected = $this->db->update(self::TABLE, $data, $where, $bindings);
        $tempRows = $this->snapshot();
        $this->assertTrue($this->usedTempTablePath(), 'Expected the staging-table JOIN path');

        $this->assertSame($caseAffected, $tempAffected, 'Affected count differs between paths');
        $this->assertSame($caseRows, $tempRows, 'Persisted rows differ between paths');

        return [$tempAffected, $tempRows];
    }

    /**
     * Batch payload of {@see ROWS} rows renaming each row by id.
     *
     * @param callable(int): array<string|int, mixed> $whereFn
     * @return array{0: list<array<string, mixed>>, 1: list<array<string|int, mixed>>}
     */
    private function payload(callable $whereFn): array
    {
        $data = [];
        $where = [];
        for ($i = 1; $i <= self::ROWS; $i++) {
            $data[] = ['name' => "Renamed {$i}"];
            $where[] = $whereFn($i);
        }

        return [$data, $where];
    }

    /** Deterministic seed: half the rows in tenant 1, statuses cycling, views = id. */
    private function seed(): void
    {
        $this->pdo->exec('TRUNCATE TABLE ' . self::TABLE);

        $rows = [];
        for ($i = 1; $i <= self::ROWS; $i++) {
            $rows[] = [
                'id' => $i,
                'tenant' => $i % 2 === 0 ? 2 : 1,
                'name' => "User {$i}",
                'status' => $this->seedStatus($i),
                'views' => $i,
            ];
        }
        $this->db->insert(self::TABLE, $rows);
        $this->db->sql = [];
    }

    private function seedStatus(int $id): string
    {
        return ['active', 'pending', 'inactive'][$id % 3];
    }

    /** @return list<array<string, mixed>> */
    private function snapshot(): array
    {
        return $this->pdo->query(
            'SELECT id, tenant, name, status, views FROM ' . self::TABLE . ' ORDER BY id'
        )->fetchAll(PDO::FETCH_ASSOC);
    }

    private function usedTempTablePath(): bool
    {
        return $this->joinUpdateSql() !== '';
    }

    /** The staged JOIN UPDATE from the last run, or '' when the batch stayed on CASE/WHEN. */
    private function joinUpdateSql(): string
    {
        foreach ($this->db->sql as $sql) {
            if (str_starts_with($sql, 'UPDATE') && str_contains($sql, 'tmp_pico_update_')) {
                return $sql;
            }
        }

        return '';
    }
}
