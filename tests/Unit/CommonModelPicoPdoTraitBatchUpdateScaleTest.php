<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/**
 * Scale / timing smoke for batch UPDATE (CASE WHEN).
 *
 * Seed + update use the real trait API against MariaDB. Timing gates are ±15–35%
 * around 20-run averages measured here with Xdebug off after the temp-table path
 * (~2.3 ms / ~22 ms / ~233 ms). Below {@see CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS}
 * CASE/WHEN is kept; at/above that threshold the JOIN path dominates.
 *
 * Marked `@group slow` — exclude with: vendor/bin/phpunit --exclude-group slow
 */
#[Group('slow')]
class CommonModelPicoPdoTraitBatchUpdateScaleTest extends TestCase
{
    private const TABLE = 'unit_trait_batch_upd_scale';

    /** Random-payload suite size — between the 2k and 20k fixed gates. */
    private const RANDOM_ROW_COUNT = 5000;

    /** Statuses drawn for random payloads (matches ENUM-style values used elsewhere). */
    private const STATUSES = ['active', 'pending', 'inactive'];

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
                name VARCHAR(255) NOT NULL,
                status VARCHAR(32) NOT NULL DEFAULT \'active\',
                views INT NOT NULL DEFAULT 0,
                touched_at DATETIME NULL
            )'
        );

        $this->db = new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                insert as public;
                update as public;
            }

            public function __construct(PDO $pdo)
            {
                $this->pdo = $pdo;
            }
        };
    }

    protected function tearDown(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        parent::tearDown();
    }

    /**
     * @return array<string, array{0: int, 1: float, 2: float}>
     */
    public static function batchUpdateScaleProvider(): array
    {
        // Gates around 20-run averages (Docker, Xdebug off, temp-table path for n ≥ 200):
        //   200 → avg 2.34 ms (1.93–3.50)
        //  2000 → avg 22.5 ms (20.5–32.3)
        // 20000 → avg 233 ms (225–247); upper widened for host load noise
        return [
            '200 rows' => [200, 0.0015, 0.0080],
            '2 000 rows' => [2000, 0.015, 0.060],
            '20 000 rows' => [20000, 0.175, 0.450],
        ];
    }

    #[DataProvider('batchUpdateScaleProvider')]
    public function testBatchUpdateCompletesWithinExpectedTime(
        int $rowCount,
        float $minSeconds,
        float $maxSeconds
    ): void {
        $this->seedRows($rowCount);

        $data = [];
        $where = [];
        for ($id = 1; $id <= $rowCount; $id++) {
            $data[] = [
                'name' => "Updated {$id}",
                'status' => 'pending',
                'views' => $id,
            ];
            $where[] = ['id' => $id];
        }

        $started = hrtime(true);
        $affected = $this->db->update(self::TABLE, $data, $where);
        $elapsed = (hrtime(true) - $started) / 1e9;

        $this->assertSame($rowCount, $affected);
        $this->assertSame(
            $rowCount,
            (int)$this->pdo->query(
                'SELECT COUNT(*) FROM ' . self::TABLE . " WHERE status = 'pending'"
            )->fetchColumn()
        );
        // Spot-check ends so a partial CASE WHEN failure cannot hide behind the count.
        $this->assertSame(
            'Updated 1',
            $this->pdo->query('SELECT name FROM ' . self::TABLE . ' WHERE id = 1')->fetchColumn()
        );
        $this->assertSame(
            "Updated {$rowCount}",
            $this->pdo->query(
                'SELECT name FROM ' . self::TABLE . ' WHERE id = ' . $rowCount
            )->fetchColumn()
        );
        $this->assertSame(
            (string)$rowCount,
            (string)$this->pdo->query(
                'SELECT views FROM ' . self::TABLE . ' WHERE id = ' . $rowCount
            )->fetchColumn()
        );

        // Timing gates are calibrated with Xdebug off; coverage mode multiplies wall time.
        if ($this->shouldAssertTiming()) {
            $this->assertElapsedWithin($elapsed, $minSeconds, $maxSeconds, $rowCount);
        }
    }

    /**
     * ~5k rows with random name / status / views / touched_at; every row must match.
     * Timing gate ≈ ±20% around a 5-run average (~61 ms with Xdebug off, temp-table path).
     */
    public function testBatchUpdateRandomPayloadAround5000Rows(): void
    {
        $n = self::RANDOM_ROW_COUNT;
        $this->seedRows($n);

        [$data, $where, $expected] = $this->buildRandomBatchPayload($n, shuffleIds: false);

        $started = hrtime(true);
        $affected = $this->db->update(self::TABLE, $data, $where);
        $elapsed = (hrtime(true) - $started) / 1e9;

        $this->assertSame($n, $affected);
        $this->assertPersistedBatch($expected);
        if ($this->shouldAssertTiming()) {
            $this->assertElapsedWithin($elapsed, 0.045, 0.130, $n);
        }
    }

    /** Same random payload, but WHERE rows are applied in shuffled id order. */
    public function testBatchUpdateRandomPayloadShuffledIdsAround5000Rows(): void
    {
        $n = self::RANDOM_ROW_COUNT;
        $this->seedRows($n);

        [$data, $where, $expected] = $this->buildRandomBatchPayload($n, shuffleIds: true);

        $affected = $this->db->update(self::TABLE, $data, $where);

        $this->assertSame($n, $affected);
        $this->assertPersistedBatch($expected);
    }

    /**
     * Mixed SET shapes across ~5k random rows: bound datetime, raw NOW(), and status-only.
     * Confirms CASE WHEN still leaves untouched columns alone per row.
     */
    public function testBatchUpdateRandomMixedSetShapesAround5000Rows(): void
    {
        $n = self::RANDOM_ROW_COUNT;
        $this->seedRows($n);

        $before = $this->pdo->query(
            'SELECT id, name, status, views, touched_at FROM ' . self::TABLE . ' ORDER BY id'
        )->fetchAll(PDO::FETCH_ASSOC);

        $data = [];
        $where = [];
        $expectName = [];
        $expectStatus = [];
        $expectViews = [];
        $expectTouchedBound = [];

        for ($id = 1; $id <= $n; $id++) {
            $shape = $id % 3;
            $where[] = ['id' => $id];

            if ($shape === 0) {
                // Bound random datetime + name/status/views.
                $touched = $this->randomDateTime();
                $row = [
                    'name' => 'Bound-' . bin2hex(random_bytes(3)),
                    'status' => self::STATUSES[random_int(0, 2)],
                    'views' => random_int(1, 50_000),
                    'touched_at' => $touched,
                ];
                $data[] = $row;
                $expectName[$id] = $row['name'];
                $expectStatus[$id] = $row['status'];
                $expectViews[$id] = $row['views'];
                $expectTouchedBound[$id] = $touched;
            } elseif ($shape === 1) {
                // Raw SQL datetime; name changes, views stay seed value.
                $row = [
                    'name' => 'Now-' . bin2hex(random_bytes(3)),
                    'status' => 'pending',
                    'touched_at = NOW()',
                ];
                $data[] = $row;
                $expectName[$id] = $row['name'];
                $expectStatus[$id] = 'pending';
                $expectViews[$id] = (int)$before[$id - 1]['views'];
            } else {
                // Status-only — name / views / touched_at must remain seed values.
                $data[] = ['status' => 'inactive'];
                $expectName[$id] = $before[$id - 1]['name'];
                $expectStatus[$id] = 'inactive';
                $expectViews[$id] = (int)$before[$id - 1]['views'];
            }
        }

        $affected = $this->db->update(self::TABLE, $data, $where);
        $this->assertSame($n, $affected);

        $after = $this->pdo->query(
            'SELECT id, name, status, views, touched_at FROM ' . self::TABLE . ' ORDER BY id'
        )->fetchAll(PDO::FETCH_ASSOC);

        foreach ($after as $row) {
            $id = (int)$row['id'];
            $this->assertSame($expectName[$id], $row['name'], "id={$id} name");
            $this->assertSame($expectStatus[$id], $row['status'], "id={$id} status");
            $this->assertSame($expectViews[$id], (int)$row['views'], "id={$id} views");

            if (isset($expectTouchedBound[$id])) {
                $this->assertSame($expectTouchedBound[$id], $row['touched_at'], "id={$id} touched_at");
            } elseif ($id % 3 === 1) {
                $this->assertNotNull($row['touched_at'], "id={$id} NOW() touched_at");
                $this->assertGreaterThan('2020-01-01 00:00:00', $row['touched_at']);
            } else {
                $this->assertNull($row['touched_at'], "id={$id} untouched touched_at");
            }
        }
    }

    /**
     * @return array{0: list<array<string, mixed>>, 1: list<array<string, int>>, 2: array<int, array<string, mixed>>}
     */
    private function buildRandomBatchPayload(int $rowCount, bool $shuffleIds): array
    {
        $ids = range(1, $rowCount);
        if ($shuffleIds) {
            shuffle($ids);
        }

        $data = [];
        $where = [];
        $expected = [];

        foreach ($ids as $id) {
            $row = [
                'name' => 'Rnd-' . bin2hex(random_bytes(4)),
                'status' => self::STATUSES[random_int(0, count(self::STATUSES) - 1)],
                'views' => random_int(0, 1_000_000),
                'touched_at' => $this->randomDateTime(),
            ];
            $data[] = $row;
            $where[] = ['id' => $id];
            $expected[$id] = $row;
        }

        return [$data, $where, $expected];
    }

    /** @param array<int, array<string, mixed>> $expected */
    private function assertPersistedBatch(array $expected): void
    {
        $rows = $this->pdo->query(
            'SELECT id, name, status, views, touched_at FROM ' . self::TABLE . ' ORDER BY id'
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(count($expected), $rows);
        foreach ($rows as $row) {
            $id = (int)$row['id'];
            $want = $expected[$id];
            $this->assertSame($want['name'], $row['name'], "id={$id} name");
            $this->assertSame($want['status'], $row['status'], "id={$id} status");
            $this->assertSame($want['views'], (int)$row['views'], "id={$id} views");
            $this->assertSame($want['touched_at'], $row['touched_at'], "id={$id} touched_at");
        }
    }

    private function assertElapsedWithin(
        float $elapsed,
        float $minSeconds,
        float $maxSeconds,
        int $rowCount
    ): void {
        $this->assertGreaterThanOrEqual(
            $minSeconds,
            $elapsed,
            sprintf(
                'Batch update of %d rows finished in %.3fs — expected at least %.3fs (timing floor)',
                $rowCount,
                $elapsed,
                $minSeconds
            )
        );
        $this->assertLessThanOrEqual(
            $maxSeconds,
            $elapsed,
            sprintf(
                'Batch update of %d rows took %.3fs — expected at most %.3fs',
                $rowCount,
                $elapsed,
                $maxSeconds
            )
        );
    }

    /** Timing gates assume Xdebug off; coverage/debug modes inflate wall time many times over. */
    private function shouldAssertTiming(): bool
    {
        $mode = (string)(getenv('XDEBUG_MODE') ?: ini_get('xdebug.mode') ?: '');

        return $mode === '' || $mode === 'off';
    }

    /** Random DATETIME string in a fixed window (bound as a normal SET value). */
    private function randomDateTime(): string
    {
        $ts = random_int(strtotime('2020-01-01 00:00:00'), strtotime('2026-07-01 23:59:59'));

        return date('Y-m-d H:i:s', $ts);
    }

    /** Seed with chunked multi-row INSERT so setup stays cheap vs the update under test. */
    private function seedRows(int $rowCount): void
    {
        $batch = [];
        for ($i = 1; $i <= $rowCount; $i++) {
            $batch[] = [
                'name' => "User {$i}",
                'status' => 'active',
                'views' => 0,
            ];
            if (count($batch) === 500 || $i === $rowCount) {
                $this->db->insert(self::TABLE, $batch);
                $batch = [];
            }
        }

        $this->assertSame(
            $rowCount,
            (int)$this->pdo->query('SELECT COUNT(*) FROM ' . self::TABLE)->fetchColumn()
        );
    }
}
