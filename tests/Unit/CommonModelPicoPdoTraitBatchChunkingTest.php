<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoTrait;
use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;

/**
 * TDD: batch INSERT / UPDATE / DELETE that exceed MySQL `max_allowed_packet` (16 MiB).
 *
 * Setup pins the server packet limit at 16 MiB. Without chunking, a single multi-row
 * statement larger than that fails with error 1153. With chunking, the same public
 * API calls must succeed and persist every row.
 *
 * These tests are expected to FAIL until batch chunking is implemented, then PASS.
 */
class CommonModelPicoPdoTraitBatchChunkingTest extends TestCase
{
    private const TABLE = 'unit_trait_chunk_users';

    /** MariaDB / MySQL default cap we pin for these tests. */
    private const PACKET_LIMIT_BYTES = 16 * 1024 * 1024;

    /**
     * Per-row payload size. Ten rows → ~20 MiB of bound data (> 16 MiB packet).
     * Kept as MEDIUMTEXT-friendly (under 16 MiB per value).
     */
    private const ROW_PAYLOAD_BYTES = 2_100_000;

    private const OVERSIZED_ROW_COUNT = 10;

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

        $this->ensureMaxAllowedPacket(self::PACKET_LIMIT_BYTES);

        $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->pdo->exec(
            'CREATE TABLE ' . self::TABLE . ' (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(64) NULL,
                status VARCHAR(64) NULL,
                payload MEDIUMTEXT NULL
            )'
        );

        $this->db = new class ($this->pdo) {
            use CommonModelPicoPdoTrait {
                insert as public;
                update as public;
                delete as public;
            }

            public function __construct(PDO $pdo)
            {
                $this->pdo = $pdo;
            }
        };
    }

    protected function tearDown(): void
    {
        try {
            $this->pdo->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        } catch (PDOException) {
            // Packet-limit failures can kill the connection; ignore cleanup errors.
        }
        parent::tearDown();
    }

    public function testServerMaxAllowedPacketIs16Mebibytes(): void
    {
        $this->assertSame(
            self::PACKET_LIMIT_BYTES,
            $this->currentMaxAllowedPacket(),
            'Chunking tests require max_allowed_packet = 16 MiB'
        );
    }

    public function testOversizedRawInsertWithoutChunkingHitsPacketLimit(): void
    {
        // Sanity: the server really rejects a >16 MiB execute when sent as one statement.
        $blob = str_repeat('X', self::ROW_PAYLOAD_BYTES);
        $placeholders = [];
        $params = [];
        for ($i = 0; $i < self::OVERSIZED_ROW_COUNT; $i++) {
            $ph = ":p{$i}";
            $placeholders[] = "({$ph})";
            $params[$ph] = $blob;
        }
        $sql = 'INSERT INTO ' . self::TABLE . ' (payload) VALUES ' . implode(', ', $placeholders);

        $this->expectException(PDOException::class);
        $this->expectExceptionMessage("Got a packet bigger than 'max_allowed_packet' bytes");

        $stmt = $this->pdo->prepare($sql);
        foreach ($params as $key => $value) {
            $stmt->bindValue($key, $value);
        }
        $stmt->execute();
    }

    public function testBatchInsertExceeding16MbSucceedsViaChunking(): void
    {
        $rows = [];
        for ($i = 0; $i < self::OVERSIZED_ROW_COUNT; $i++) {
            $rows[] = [
                'name' => "ins_{$i}",
                'status' => 'active',
                'payload' => $this->payloadMarker("INS{$i}"),
            ];
        }

        // Must not throw 1153; every row must land.
        $meta = $this->db->insert(self::TABLE, $rows, ['meta' => true]);

        $this->assertSame(self::OVERSIZED_ROW_COUNT, $meta['rows']);
        $this->assertSame(
            self::OVERSIZED_ROW_COUNT,
            (int)$this->pdo->query('SELECT COUNT(*) FROM ' . self::TABLE)->fetchColumn()
        );
        for ($i = 0; $i < self::OVERSIZED_ROW_COUNT; $i++) {
            $got = $this->pdo->query(
                'SELECT LEFT(payload, 16) FROM ' . self::TABLE . " WHERE name = 'ins_{$i}'"
            )->fetchColumn();
            $this->assertSame(substr($this->payloadMarker("INS{$i}"), 0, 16), $got);
        }
    }

    public function testBatchUpdateExceeding16MbSucceedsViaChunking(): void
    {
        // Seed small rows first (fits in one packet), then batch-update with huge SET values.
        for ($i = 1; $i <= self::OVERSIZED_ROW_COUNT; $i++) {
            $this->pdo->exec(
                'INSERT INTO ' . self::TABLE . " (id, name, status, payload) VALUES ({$i}, 'u{$i}', 'old', 'tiny')"
            );
        }

        $data = [];
        $where = [];
        for ($i = 1; $i <= self::OVERSIZED_ROW_COUNT; $i++) {
            $data[] = [
                'status' => 'new',
                'payload' => $this->payloadMarker("UPD{$i}"),
            ];
            $where[] = ['id' => $i];
        }

        $affected = $this->db->update(self::TABLE, $data, $where);

        $this->assertSame(self::OVERSIZED_ROW_COUNT, $affected);
        $this->assertSame(
            self::OVERSIZED_ROW_COUNT,
            (int)$this->pdo->query("SELECT COUNT(*) FROM " . self::TABLE . " WHERE status = 'new'")->fetchColumn()
        );
        for ($i = 1; $i <= self::OVERSIZED_ROW_COUNT; $i++) {
            $got = $this->pdo->query(
                'SELECT LEFT(payload, 16) FROM ' . self::TABLE . " WHERE id = {$i}"
            )->fetchColumn();
            $this->assertSame(substr($this->payloadMarker("UPD{$i}"), 0, 16), $got);
        }
    }

    public function testBatchDeleteExceeding16MbSucceedsViaChunking(): void
    {
        // Seed rows that each carry a unique large payload, then delete by matching those payloads.
        $where = [];
        for ($i = 0; $i < self::OVERSIZED_ROW_COUNT; $i++) {
            $payload = $this->payloadMarker("DEL{$i}");
            $stmt = $this->pdo->prepare(
                'INSERT INTO ' . self::TABLE . ' (name, status, payload) VALUES (?, ?, ?)'
            );
            $stmt->execute(["d{$i}", 'gone', $payload]);
            $where[] = ['payload' => $payload];
        }
        // Keep one row that must survive.
        $this->pdo->exec(
            'INSERT INTO ' . self::TABLE . " (name, status, payload) VALUES ('keep', 'keep', 'small')"
        );

        $affected = $this->db->delete(self::TABLE, $where);

        $this->assertSame(self::OVERSIZED_ROW_COUNT, $affected);
        $this->assertSame(
            1,
            (int)$this->pdo->query('SELECT COUNT(*) FROM ' . self::TABLE)->fetchColumn()
        );
        $this->assertSame(
            'keep',
            $this->pdo->query('SELECT name FROM ' . self::TABLE)->fetchColumn()
        );
    }

    public function testMultiChunkInsertRollsBackWhenALaterChunkFails(): void
    {
        // UNIQUE on name: first chunks insert, last chunk collides → owned transaction rolls back all.
        $this->pdo->exec('ALTER TABLE ' . self::TABLE . ' ADD UNIQUE KEY uq_chunk_name (name)');

        $rows = [];
        for ($i = 0; $i < self::OVERSIZED_ROW_COUNT; $i++) {
            $rows[] = [
                'name' => "dup_{$i}",
                'status' => 'active',
                'payload' => $this->payloadMarker("DUP{$i}"),
            ];
        }
        // Force a duplicate that lands in a later half-chunk after size splitting.
        $rows[self::OVERSIZED_ROW_COUNT - 1]['name'] = $rows[0]['name'];

        try {
            $this->db->insert(self::TABLE, $rows);
            $this->fail('Expected duplicate-key failure after the first chunk(s) succeeded');
        } catch (PDOException $e) {
            $this->assertTrue(
                str_contains($e->getMessage(), 'Duplicate') || (int)$e->errorInfo[1] === 1062,
                $e->getMessage()
            );
        }

        // Clear a leftover aborted transaction if the driver still reports one after rollback.
        if ($this->pdo->inTransaction()) {
            $this->pdo->rollBack();
        }

        $this->assertSame(
            0,
            (int)$this->pdo->query('SELECT COUNT(*) FROM ' . self::TABLE)->fetchColumn(),
            'Failed multi-chunk insert must leave no rows (atomic rollback)'
        );
    }

    // ——— helpers ———

    /** Distinct ~2.1 MiB blob so rows stay unique under equality deletes/updates. */
    private function payloadMarker(string $tag): string
    {
        $prefix = str_pad($tag, 16, '_');
        return $prefix . str_repeat('Z', self::ROW_PAYLOAD_BYTES - strlen($prefix));
    }

    private function currentMaxAllowedPacket(): int
    {
        $row = $this->pdo->query("SHOW VARIABLES LIKE 'max_allowed_packet'")->fetch(PDO::FETCH_ASSOC);
        return (int)$row['Value'];
    }

    /**
     * Pin max_allowed_packet to 16 MiB for this suite.
     * SESSION is read-only on MariaDB; GLOBAL may require root — fall back to asserting current value.
     */
    private function ensureMaxAllowedPacket(int $bytes): void
    {
        if ($this->currentMaxAllowedPacket() === $bytes) {
            return;
        }

        try {
            $root = new PDO(
                'mysql:host=' . getenv('DB_HOST'),
                'root',
                'root_password',
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
            );
            $root->exec('SET GLOBAL max_allowed_packet = ' . $bytes);
            // Reconnect so the session picks up the new global default.
            $this->pdo = new PDO(
                'mysql:host=' . getenv('DB_HOST') . ';dbname=' . getenv('DB_NAME'),
                getenv('DB_USER'),
                getenv('DB_PASS'),
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
            );
        } catch (PDOException) {
            // Ignore — assertion below reports the mismatch.
        }

        $this->assertSame(
            $bytes,
            $this->currentMaxAllowedPacket(),
            'Unable to set max_allowed_packet to 16 MiB; configure the test DB accordingly'
        );
    }
}
