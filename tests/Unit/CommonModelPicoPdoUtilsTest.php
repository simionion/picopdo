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

    public function testBuildTempTableUpdateFallbackBranches(): void
    {
        $min = CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS;
        [$data, $where] = $this->payload($min);

        $this->assertNotSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, null, null));

        // Too few rows
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            array_slice($data, 0, $min - 1),
            array_slice($where, 0, $min - 1),
            null,
            null
        ));

        // sqlTail / invalid table
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, null, 'LIMIT 1'));
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('u.id', $data, $where, null, null));

        // Empty WHERE / SET maps, or join key also in SET
        $badWhere = array_fill(0, $min, []);
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $badWhere, null, null));
        $overlap = array_map(static fn (int $i): array => ['id' => $i, 'name' => "n{$i}"], range(1, $min));
        $overlapWhere = array_map(static fn (int $i): array => ['id' => $i], range(1, $min));
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $overlap, $overlapWhere, null, null));

        // Non-identifier column key (forces CASE/WHEN, not temp table).
        $qData = array_fill(0, $min, ['name = ?' => 'x']);
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $qData, $where, null, null));

        // Mismatched row shapes
        $mixed = $data;
        $mixed[0] = ['name' => 'a', 'role' => 'admin'];
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $mixed, $where, null, null));

        // Non-scalar value
        $nonScalar = $data;
        $nonScalar[0] = ['name' => ['not', 'scalar']];
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $nonScalar, $where, null, null));

        // Duplicate WHERE conditions
        $dupWhere = $where;
        $dupWhere[1] = $where[0];
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $dupWhere, null, null));

        // A string WHERE row cannot become a join key.
        $stringWhere = $where;
        $stringWhere[0] = 'id = 1';
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $stringWhere, null, null));

        // Empty WHERE list: peel returns no per-row keys (distinct from identical rows → empty maps).
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, [], null, null));

        // Every WHERE row identical: the peel leaves no per-row key to join on.
        $allCommon = array_fill(0, $min, ['status' => 'active']);
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $allCommon, null, null));

        // A scalar binding can only feed the single-column shorthand, never a batch clause.
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, 5, null));

        // A shared fragment that would leave an ambiguous bare reference to a staged column.
        [$ambiguous, $ambiguousWhere] = $this->payload(
            whereFn: static fn (int $i): array => ['id' => $i, 'tenant = 1 AND name IS NOT NULL']
        );
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $ambiguous,
            $ambiguousWhere,
            null,
            null
        ));

        // An already table-qualified shared key cannot be rewritten onto the alias.
        [, $dotted] = $this->payload(whereFn: static fn (int $i): array => ['id' => $i, 'users.tenant' => 1]);
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $dotted, null, null));
    }

    /** The plan carries the quoted target, the peeled WHERE and the per-row join rows. */
    public function testBuildTempTableUpdatePlanShape(): void
    {
        [$data, $where] = $this->payload();

        $plan = CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, null, null);

        $this->assertSame('`users`', $plan['target']);
        $this->assertSame([], $plan['common']);
        $this->assertSame([], $plan['bindings']);
        $this->assertCount(count($data), $plan['rowWhere']);
        $this->assertSame(['id' => 1], $plan['rowWhere'][0]);
    }

    /**
     * Conditions identical on every WHERE row are peeled off and qualified onto alias `t`;
     * only what varies stays behind as a join key.
     */
    public function testBuildTempTableUpdatePeelsCommonConditions(): void
    {
        [$data, $where] = $this->payload(
            whereFn: static fn (int $i): array => ['id' => $i, 'tenant' => 7, 'status' => 'active']
        );

        $plan = CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, null, null);

        $this->assertSame(['t.tenant' => 7, 't.status' => 'active'], $plan['common']);
        $this->assertSame(['id' => 1], $plan['rowWhere'][0]);
        $this->assertSame([], $plan['bindings']);
    }

    /** A raw common fragment is qualified too, and its `:name` binds come from the shared map. */
    public function testBuildTempTableUpdateSharedNamedBindingsMap(): void
    {
        [$data, $where] = $this->payload(
            whereFn: static fn (int $i): array => ['id' => $i, 'views > :min']
        );

        $plan = CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, [':min' => 100], null);

        $this->assertSame(['t.views > :min'], $plan['common']);
        $this->assertSame([':min' => 100], $plan['bindings']);

        // Bindings the shared clause never references are dropped, not carried into the JOIN.
        $plan = CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $data,
            $where,
            [':min' => 100, ':unused' => 'x'],
            null
        );
        $this->assertSame([':min' => 100], $plan['bindings']);

        // A placeholder with no binding at all must fall back — never bind nothing silently.
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $data,
            $where,
            [':other' => 1],
            null
        ));
    }

    /**
     * A per-row list of bindings maps is reduced the same way the WHERE rows are: identical on
     * every row feeds the shared clause, anything varying sends the batch back to CASE/WHEN.
     */
    public function testBuildTempTableUpdatePerRowBindingsMapList(): void
    {
        $min = CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS;
        [$data, $where] = $this->payload(
            whereFn: static fn (int $i): array => ['id' => $i, 'status = :st']
        );

        $plan = CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $data,
            $where,
            array_fill(0, $min, [':st' => 'active']),
            null
        );
        $this->assertSame(['t.status = :st'], $plan['common']);
        $this->assertSame([':st' => 'active'], $plan['bindings']);

        // Same name, different value per row: only CASE/WHEN can give each row its own value.
        $varying = array_map(
            static fn (int $i): array => [':st' => $i % 2 === 0 ? 'active' : 'pending'],
            range(1, $min)
        );
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, $varying, null));

        // A list shorter than the batch is a per-row list with holes, not a shared map.
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $data,
            $where,
            [[':st' => 'active']],
            null
        ));

        // A per-row list of scalars never resolves a named placeholder.
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $data,
            $where,
            range(1, $min),
            null
        ));
    }

    /**
     * Regression: a peeled raw fragment carrying a positional `?` draws on the bindings list.
     * Resolving only `:name` binds left the `?` unbound, and the batch died in buildSqlClause()
     * with "Not enough positional bindings" at exactly TEMP_UPDATE_MIN_ROWS rows — the same
     * payload one row short went through CASE/WHEN and worked.
     */
    public function testBuildTempTableUpdateResolvesPositionalCommonBindings(): void
    {
        $min = CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS;
        [$data, $where] = $this->payload(
            whereFn: static fn (int $i): array => ['id' => $i, 'views > ?']
        );

        $plan = CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $data,
            $where,
            array_fill(0, $min, [10]),
            null
        );
        $this->assertSame(['t.views > ?'], $plan['common']);
        $this->assertSame([10], $plan['bindings']);

        // Values varying per row cannot collapse into one shared clause.
        $varying = array_map(static fn (int $i): array => [$i], range(1, $min));
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, $varying, null));

        // No bindings at all leaves the `?` unfed.
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $where, null, null));

        // A named map cannot feed a positional placeholder.
        $this->assertSame([], CommonModelPicoPdoUtils::buildTempTableUpdate(
            'users',
            $data,
            $where,
            [':min' => 10],
            null
        ));

        // A keyed `col > ?` entry carries its own value, so it needs no bindings list.
        [$data, $keyed] = $this->payload(
            whereFn: static fn (int $i): array => ['id' => $i, 'views > ?' => 10]
        );
        $plan = CommonModelPicoPdoUtils::buildTempTableUpdate('users', $data, $keyed, null, null);
        $this->assertSame(['t.views > ?' => 10], $plan['common']);
        $this->assertSame([], $plan['bindings']);
    }

    public function testQualifyWhereForAlias(): void
    {
        $this->assertSame(
            ['t.id' => 1, 't.views > ?' => 5, 't.status = :st'],
            CommonModelPicoPdoUtils::qualifyWhereForAlias(['id' => 1, 'views > ?' => 5, 'status = :st'], 't')
        );
        $this->assertSame(
            ['t.id' => 1],
            CommonModelPicoPdoUtils::qualifyWhereForAlias(['`id`' => 1], 't')
        );

        // Nothing to qualify stays empty; a fragment with no leading column cannot be qualified.
        $this->assertSame([], CommonModelPicoPdoUtils::qualifyWhereForAlias([], 't'));
        $this->assertSame([], CommonModelPicoPdoUtils::qualifyWhereForAlias(['(a = 1 OR b = 2)'], 't'));
        $this->assertSame([], CommonModelPicoPdoUtils::qualifyWhereForAlias([5], 't'));

        // Already table-qualified: the alias has replaced the table name, so neither form works.
        $this->assertSame([], CommonModelPicoPdoUtils::qualifyWhereForAlias(['users.tenant' => 1], 't'));
        $this->assertSame([], CommonModelPicoPdoUtils::qualifyWhereForAlias(['`users`.`tenant` = 1'], 't'));

        // Only the leading column is prefixed; a later reference stays bare …
        $this->assertSame(
            ['t.views > 100 AND views < 200'],
            CommonModelPicoPdoUtils::qualifyWhereForAlias(['views > 100 AND views < 200'], 't', ['id', 'name'])
        );
        // … which is refused once that column is also on the staging side (ambiguous to MySQL).
        $this->assertSame(
            [],
            CommonModelPicoPdoUtils::qualifyWhereForAlias(['tenant = 1 AND views > 100'], 't', ['id', 'views'])
        );
    }

    public function testHasBareColumnReference(): void
    {
        $this->assertTrue(CommonModelPicoPdoUtils::hasBareColumnReference('t.a = 1 AND views > 2', ['views']));
        $this->assertTrue(CommonModelPicoPdoUtils::hasBareColumnReference('t.a = 1 AND `views` > 2', ['views']));
        $this->assertTrue(CommonModelPicoPdoUtils::hasBareColumnReference('t.a = 1 AND VIEWS > 2', ['views']));

        // Qualified references and named placeholders cannot be ambiguous.
        $this->assertFalse(CommonModelPicoPdoUtils::hasBareColumnReference('t.views > 2', ['views']));
        $this->assertFalse(CommonModelPicoPdoUtils::hasBareColumnReference('t.a = :views', ['views']));
        $this->assertFalse(CommonModelPicoPdoUtils::hasBareColumnReference('t.a = :b', []));

        // Substrings of longer identifiers are not references to the column.
        $this->assertFalse(CommonModelPicoPdoUtils::hasBareColumnReference('t.a = 1 AND view_count > 2', ['view']));
    }

    public function testSharedRowBindings(): void
    {
        // Nothing referenced, nothing required.
        $this->assertSame([], CommonModelPicoPdoUtils::sharedRowBindings([], null, 3));
        $this->assertSame([], CommonModelPicoPdoUtils::sharedRowBindings([], [':x' => 1], 3));

        $this->assertSame(
            [':x' => 1],
            CommonModelPicoPdoUtils::sharedRowBindings(['t.a = :x'], [':x' => 1], 3)
        );
        $this->assertSame(
            [':x' => 1],
            CommonModelPicoPdoUtils::sharedRowBindings(['t.a = :x'], [[':x' => 1], [':x' => 1], [':x' => 1]], 3)
        );
        $this->assertSame(
            [7],
            CommonModelPicoPdoUtils::sharedRowBindings(['t.a > ?'], [[7], [7], [7]], 3)
        );

        // Varying per row, wrong row count, scalar, or missing entirely.
        $this->assertNull(CommonModelPicoPdoUtils::sharedRowBindings(['t.a = :x'], [[':x' => 1], [':x' => 2], [':x' => 1]], 3));
        $this->assertNull(CommonModelPicoPdoUtils::sharedRowBindings(['t.a = :x'], [[':x' => 1]], 3));
        $this->assertNull(CommonModelPicoPdoUtils::sharedRowBindings(['t.a = :x'], 5, 3));
        $this->assertNull(CommonModelPicoPdoUtils::sharedRowBindings(['t.a = :x'], null, 3));
        $this->assertNull(CommonModelPicoPdoUtils::sharedRowBindings(['t.a > ?'], null, 3));

        // A second `?` needs a second positional value at the next index.
        $this->assertSame(
            [3, 9],
            CommonModelPicoPdoUtils::sharedRowBindings(['t.a > ?', 't.b < ?'], [[3, 9], [3, 9]], 2)
        );
        $this->assertNull(
            CommonModelPicoPdoUtils::sharedRowBindings(['t.a > ?', 't.b < ?'], [[3, 9], [3, 8]], 2)
        );
    }

    public function testNamedAndPositionalPlaceholdersInWhere(): void
    {
        $this->assertSame(
            [':b', ':st'],
            CommonModelPicoPdoUtils::namedPlaceholdersInWhere(['t.a > :b' => 1, 't.status = :st'])
        );
        $this->assertSame([], CommonModelPicoPdoUtils::namedPlaceholdersInWhere(['t.id' => 1]));

        // Only numeric-keyed raw fragments draw on the positional list; a keyed `?` binds itself.
        $this->assertSame(
            3,
            CommonModelPicoPdoUtils::positionalPlaceholdersInWhere(['t.a > ?', 't.b < ? AND t.c > ?'])
        );
        $this->assertSame(0, CommonModelPicoPdoUtils::positionalPlaceholdersInWhere(['t.a > ?' => 1]));
        $this->assertSame(0, CommonModelPicoPdoUtils::positionalPlaceholdersInWhere(['t.id' => 1]));
    }

    public function testIsolateNamedPlaceholders(): void
    {
        // No bound map: every user name is renamed under the prefix.
        [$sql, $params] = CommonModelPicoPdoUtils::isolateNamedPlaceholders(
            't.status = :st AND t.role = :st2',
            [':st' => 'active', ':st2' => 'admin'],
            'tmp_c_'
        );
        $this->assertSame('t.status = :tmp_c_st AND t.role = :tmp_c_st2', $sql);
        $this->assertSame([':tmp_c_st' => 'active', ':tmp_c_st2' => 'admin'], $params);

        // Already prefixed names are left alone.
        [$sql, $params] = CommonModelPicoPdoUtils::isolateNamedPlaceholders(
            't.a = :tmp_c_0',
            [':tmp_c_0' => 1],
            'tmp_c_'
        );
        $this->assertSame('t.a = :tmp_c_0', $sql);
        $this->assertSame([':tmp_c_0' => 1], $params);

        // With a bound map, only a name re-bound to a *different* value is renamed.
        [$sql, $params] = CommonModelPicoPdoUtils::isolateNamedPlaceholders(
            'a = :x AND b = :y',
            [':x' => 1, ':y' => 2],
            'b1_n_',
            [':x' => 1, ':y' => 99]
        );
        $this->assertSame('a = :x AND b = :b1_n_y', $sql);
        $this->assertSame([':x' => 1, ':b1_n_y' => 2], $params);
    }

    public function testSplitCommonWhereRows(): void
    {
        [$common, $rows] = CommonModelPicoPdoUtils::splitCommonWhereRows([
            ['id' => 1, 'tenant' => 7, 'status' => 'active'],
            ['id' => 2, 'tenant' => 7, 'status' => 'pending'],
            ['id' => 3, 'tenant' => 7, 'status' => 'active'],
        ]);
        $this->assertSame(['tenant' => 7], $common);
        $this->assertSame(['id' => 1, 'status' => 'active'], $rows[0]);
        $this->assertSame(['id' => 2, 'status' => 'pending'], $rows[1]);

        // Identity, not loose equality: '7' and 7 are not the same shared value.
        [$common] = CommonModelPicoPdoUtils::splitCommonWhereRows([['t' => 7], ['t' => '7']]);
        $this->assertSame([], $common);

        // A single row is entirely common.
        [$common, $rows] = CommonModelPicoPdoUtils::splitCommonWhereRows([['id' => 1]]);
        $this->assertSame(['id' => 1], $common);
        $this->assertSame([[]], $rows);
    }

    /**
     * Batch payload of `$rowCount` equal-shaped rows: `['name' => 'nI']` set per row.
     *
     * @return array{0: list<array<string, mixed>>, 1: list<array<string|int, mixed>>}
     */
    private function payload(int|null $rowCount = null, callable|null $whereFn = null): array
    {
        $rowCount ??= CommonModelPicoPdoUtils::TEMP_UPDATE_MIN_ROWS;
        $whereFn ??= static fn (int $i): array => ['id' => $i];

        $data = [];
        $where = [];
        for ($i = 1; $i <= $rowCount; $i++) {
            $data[] = ['name' => "n{$i}"];
            $where[] = $whereFn($i);
        }

        return [$data, $where];
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
