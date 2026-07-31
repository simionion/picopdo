<?php

declare(strict_types=1);

namespace Lodur\PicoPdo\Tests;

use Lodur\PicoPdo\CommonModelPicoPdoUtils;
use PHPUnit\Framework\TestCase;

/**
 * Pure unit coverage for {@see CommonModelPicoPdoUtils} (no database).
 */
class CommonModelPicoPdoUtilsTest extends TestCase
{
    public function testAppendLimitOneAddsLimitWhenMissing(): void
    {
        $this->assertSame('LIMIT 1', CommonModelPicoPdoUtils::appendLimitOne(null));
        $this->assertSame('ORDER BY id LIMIT 1', CommonModelPicoPdoUtils::appendLimitOne('ORDER BY id'));
        $this->assertSame('ORDER BY id LIMIT 5', CommonModelPicoPdoUtils::appendLimitOne('ORDER BY id LIMIT 5'));
    }

    public function testHasLimitDetectsClause(): void
    {
        $this->assertFalse(CommonModelPicoPdoUtils::hasLimit(null));
        $this->assertFalse(CommonModelPicoPdoUtils::hasLimit('ORDER BY id'));
        $this->assertTrue(CommonModelPicoPdoUtils::hasLimit('LIMIT 10'));
        $this->assertTrue(CommonModelPicoPdoUtils::hasLimit('order by id limit 1'));
    }

    public function testHasMultipleInsertShapesSkipsEmptyRowsAndDetectsDiff(): void
    {
        $this->assertFalse(CommonModelPicoPdoUtils::hasMultipleInsertShapes([
            [],
            ['name' => 'A'],
            [],
            ['name' => 'B'],
        ]));
        $this->assertTrue(CommonModelPicoPdoUtils::hasMultipleInsertShapes([
            ['name' => 'A'],
            ['name' => 'B', 'role' => 'admin'],
        ]));
        $this->assertTrue(CommonModelPicoPdoUtils::hasMultipleInsertShapes([
            ['name' => 'A', 'created_at = NOW()'],
            ['name' => 'B', 'role' => 'admin'],
        ]));
    }

    public function testNormalizePositionalBindings(): void
    {
        $this->assertNull(CommonModelPicoPdoUtils::normalizePositionalBindings([]));
        $this->assertSame([1, 2], CommonModelPicoPdoUtils::normalizePositionalBindings([2 => 1, 5 => 2]));
        $this->assertSame([':id' => 1], CommonModelPicoPdoUtils::normalizePositionalBindings([':id' => 1]));
        $this->assertSame(5, CommonModelPicoPdoUtils::normalizePositionalBindings(5));
    }

    public function testNormalizeTableNameAcceptsPlainAndBackticked(): void
    {
        $this->assertSame('users', CommonModelPicoPdoUtils::normalizeTableName('users'));
        $this->assertSame('users', CommonModelPicoPdoUtils::normalizeTableName('`users`'));
    }

    public function testNormalizeTableNameRejectsInvalid(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid table name');
        CommonModelPicoPdoUtils::normalizeTableName('users; DROP TABLE x');
    }

    public function testQuoteIdentifier(): void
    {
        $this->assertSame('`users`', CommonModelPicoPdoUtils::quoteIdentifier('users'));
        $this->assertNull(CommonModelPicoPdoUtils::quoteIdentifier('u.id'));
        $this->assertNull(CommonModelPicoPdoUtils::quoteIdentifier(0));
        $this->assertNull(CommonModelPicoPdoUtils::quoteIdentifier('id = ?'));
    }

    public function testRequireWhereClause(): void
    {
        $this->assertSame('WHERE id = 1', CommonModelPicoPdoUtils::requireWhereClause('id = 1', 'UPDATE'));
        $this->assertSame('WHERE id = 1', CommonModelPicoPdoUtils::requireWhereClause('WHERE id = 1', 'DELETE'));

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('UPDATE requires a WHERE condition');
        CommonModelPicoPdoUtils::requireWhereClause('   ', 'UPDATE');
    }

    public function testSplitAssignmentHappyPathAndErrors(): void
    {
        $this->assertSame(['created_at', 'NOW()'], CommonModelPicoPdoUtils::splitAssignment('created_at = NOW()'));

        try {
            CommonModelPicoPdoUtils::splitAssignment('not-an-assignment');
            $this->fail('Expected InvalidArgumentException');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringContainsString("must contain '='", $e->getMessage());
        }

        try {
            CommonModelPicoPdoUtils::splitAssignment(' = NOW()');
            $this->fail('Expected InvalidArgumentException');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringContainsString('incomplete', $e->getMessage());
        }

        try {
            CommonModelPicoPdoUtils::splitAssignment('created_at = ');
            $this->fail('Expected InvalidArgumentException');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringContainsString('incomplete', $e->getMessage());
        }
    }

    public function testCanUpdateViaTempTableFalseBranches(): void
    {
        $min = CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS;
        $data = [];
        $where = [];
        for ($i = 0; $i < $min; $i++) {
            $data[] = ['name' => "n{$i}"];
            $where[] = ['id' => $i + 1];
        }

        $this->assertTrue(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $data, $where, null, null));

        // Too few rows
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable(
            'users',
            array_slice($data, 0, $min - 1),
            array_slice($where, 0, $min - 1),
            null,
            null
        ));

        // Bindings / sqlTail / invalid table
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $data, $where, [':x' => 1], null));
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $data, $where, null, 'LIMIT 1'));
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('u.id', $data, $where, null, null));

        // Empty WHERE / SET maps, or join key also in SET
        $badWhere = array_fill(0, $min, []);
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $data, $badWhere, null, null));
        $overlap = array_map(static fn (int $i): array => ['id' => $i, 'name' => "n{$i}"], range(1, $min));
        $overlapWhere = array_map(static fn (int $i): array => ['id' => $i], range(1, $min));
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $overlap, $overlapWhere, null, null));

        // Non-identifier column key (forces CASE/WHEN, not temp table).
        $qData = array_fill(0, $min, ['name = ?' => 'x']);
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $qData, $where, null, null));

        // Mismatched row shapes
        $mixed = $data;
        $mixed[0] = ['name' => 'a', 'role' => 'admin'];
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $mixed, $where, null, null));

        // Non-scalar value
        $nonScalar = $data;
        $nonScalar[0] = ['name' => ['not', 'scalar']];
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $nonScalar, $where, null, null));

        // Duplicate WHERE conditions
        $dupWhere = $where;
        $dupWhere[1] = $where[0];
        $this->assertFalse(CommonModelPicoPdoUtils::canUpdateViaTempTable('users', $data, $dupWhere, null, null));
    }

    public function testChunkListAndChunkForPacket(): void
    {
        $this->assertSame(['a', 'b'], CommonModelPicoPdoUtils::chunkList(['a', 'b', 'c'], 0, 2));
        $this->assertSame('shared', CommonModelPicoPdoUtils::chunkList('shared', 0, 2));
        $this->assertNull(CommonModelPicoPdoUtils::chunkList(null, 0, 2));

        $small = [['x' => 1], ['x' => 2]];
        $this->assertSame([$small], CommonModelPicoPdoUtils::chunkForPacket($small));

        // Force a split with a large fixed overhead so two tiny rows still exceed the fill ratio.
        $chunks = CommonModelPicoPdoUtils::chunkForPacket(
            [['x' => 1], ['x' => 2]],
            (int)(CommonModelPicoPdoUtils::MAX_ALLOWED_PACKET * CommonModelPicoPdoUtils::PACKET_FILL_RATIO)
        );
        $this->assertCount(2, $chunks);
    }
}
