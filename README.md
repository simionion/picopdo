# PicoPdo

A lightweight PDO trait for PHP models that provides common database operations with automatic placeholder conversion and flexible query building.

## Features

- **Automatic `?` to named placeholder conversion** - All positional placeholders are converted to named placeholders
- **Array-expansion for `IN` clauses** - Supports both `IN (:ids)` and `IN (?)` syntax
- **Flexible WHERE clause building** - Arrays, raw SQL, shorthand column filters all supported
- **Raw SQL in arrays** - Use numeric keys for raw SQL strings
- **Keys with `?` placeholders** - Direct binding syntax: `['date > ?' => $date]`
- **INSERT modes** - INSERT, REPLACE, INSERT IGNORE, ON DUPLICATE KEY UPDATE
- **100% test coverage** across unit and integration tests

## Requirements

- PHP 8.2 or higher
- PDO PHP extension
- MariaDB/MySQL

## Installation

```bash
git clone https://github.com/simionion/picopdo.git
cd picopdo
make install
```

## Development Setup

```bash
make start    # Build, start containers, and install dependencies
make test     # Run tests with coverage report
```

## Usage

### Basic Setup

```php
use Lodur\PicoPdo\CommonModelPicoPdoTrait;

class UserModel
{
    use CommonModelPicoPdoTrait;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }
}
```

## Key Features

### 1. Automatic `?` to Named Placeholder Conversion

All `?` placeholders are automatically converted to named placeholders:

```php
// You write:
$users = $model->selectAll('users', null, 'email = ? AND status = ?', ['user@example.com', 'active']);

// Generates: WHERE email = :where_0 AND status = :where_1
```

### 2. Array-Expansion for `IN` Clauses

Two syntaxes supported:

```php
// Named placeholder (original)
$users = $model->selectAll('users', null, 'id IN (:ids)', [':ids' => [1, 2, 3]]);
// Expands to: id IN (:ids0, :ids1, :ids2)

// Positional placeholder (new)
$users = $model->selectAll('users', null, 'id IN (?)', [[1, 2, 3]]);
// Expands to: id IN (:where_00, :where_01, :where_02)
```

### 3. Flexible WHERE Clause Building

**Arrays with raw SQL (numeric keys):**
```php
$users = $model->selectAll('users', null, [
    'status' => 'active',
    'email_verified != 0',        // Raw SQL (numeric key)
    'created_at > :date'          // Raw SQL with named placeholder
], [':date' => '2024-01-01']);
```

**Keys with `?` placeholders:**
```php
$users = $model->selectAll('users', null, [
    'status' => 'active',
    'created_at > ?' => '2024-01-01'    // Key contains ?, value is binding
]);
// Generates: WHERE status = :where_status AND created_at > :where_0
```

### 4. INSERT Modes

```php
// Basic INSERT
$id = $model->insert('users', ['name' => 'John', 'email' => 'john@example.com']);

// REPLACE INTO
$id = $model->insertReplace('users', ['name' => 'John', 'email' => 'john@example.com']);

// INSERT IGNORE
$result = $model->insertIgnore('users', ['name' => 'John', 'email' => 'john@example.com']);
// Returns: ['id' => 123|0, 'rows' => 1|0, 'status' => 'inserted'|'noop']

// ON DUPLICATE KEY UPDATE
$result = $model->insertOnDuplicateKeyUpdate('users',
    ['name' => 'John', 'email' => 'john@example.com'],
    ['name' => 'John', 'status' => 'active']  // Update values
);
// Returns: ['id' => 123|0, 'rows' => 1|2|0, 'status' => 'inserted'|'updated'|'noop']
```

For queries with JOINs or other SQL the helpers do not cover, use **`prepExec()`** with bound parameters (same `?` → named conversion applies).

## Common Operations

### SELECT

```php
// Simple
$user = $model->selectOne('users', ['name', 'email'], 'id', 1);
$users = $model->selectAll('users', ['name', 'email'], ['status' => 'active']);

// With extra query suffix
$users = $model->selectAll('users', ['name'], ['status' => 'active'], null, 'ORDER BY name LIMIT 10');
```

### UPDATE

```php
// Simple
$affected = $model->update('users', ['name' => 'John'], 'id', 1);

// With raw SQL in SET (numeric key)
$affected = $model->update('users',
    ['name' => 'John', 'date_verified = NOW()'],
    'id', 1
);

// With a SQL tail (applies to the whole statement)
$affected = $model->update('users', ['status' => 'archived'], ['status' => 'inactive'], null, 'ORDER BY id LIMIT 10');

// Batch (one query, parallel lists — build in a loop, then pass both)
// Generated as CASE WHEN per column; single updates keep classic `SET col = :set_col` SQL.
$data = [];
$where = [];
foreach ($rows as $row) {
    $data[] = ['name' => $row['name'], 'status' => $row['status']];
    $where[] = ['id' => $row['id']];
}
$affected = $model->update('users', $data, $where);
// Note: in batch mode a LIMIT in the sqlTail caps the total rows changed across all entries.
```

### DELETE

```php
$affected = $model->delete('users', 'id', 1);
$affected = $model->delete('users', ['status' => 'inactive', 'email_verified' => 0]);
```

### EXISTS

```php
$exists = $model->exists('users', 'id', 1);
$exists = $model->exists('users', ['email' => 'john@example.com', 'status' => 'active']);
```

## Complex Example: All Features Combined

Here's a comprehensive example showcasing flexible WHERE, `IN` expansion, `?` bindings, and `sqlTail`:

```php
$maxLastLogin = '2024-06-01';

$users = $model->selectAll(
    'users',
    ['id', 'name', 'email'],
    [
        'status' => 'active',
        'name != ""',
        'created_at > :min_date',
        'created_at < ?' => $maxLastLogin,
        'id IN (:user_ids)',
    ],
    [
        ':min_date' => '2024-01-01',
        ':user_ids' => [1, 2, 3],
    ],
    'ORDER BY name LIMIT 10'
);
```

## Walkthrough

This section walks through the trait API in order of complexity. Every CRUD helper (`exists`, `select*`, `insert*`, `update`, `delete`) shares the same `$where` / `$bindings` rules; learn them once, use them everywhere.

Methods are `protected` in the trait — expose them from your model (or alias them) as needed.

### Step 1 — Column shorthand (simplest filter)

When you filter on **one column with equality**, pass the column name as `$where` and the value as `$bindings`:

```php
$user = $model->selectOne('users', ['name', 'email'], 'id', 1);
// → WHERE id = :where_id

$exists = $model->exists('users', 'email', 'john@example.com');
$affected = $model->update('users', ['name' => 'John'], 'id', 5);
$affected = $model->delete('users', 'id', 5);
```

**Use when:** one equality condition, no raw SQL.

---

### Step 2 — Associative `$where` (multiple AND conditions, no extra bindings)

Pass an array; each `'column' => value` becomes `column = :where_column`. Values are bound automatically — **`$bindings` can be omitted**:

```php
$users = $model->selectAll('users', null, [
    'status' => 'active',
    'email_verified' => 1,
]);
// → WHERE status = :where_status AND email_verified = :where_email_verified
```

**Use when:** several simple equalities, no expressions.

---

### Step 3 — Raw SQL fragments (numeric keys)

Add conditions the trait cannot express as `'col' => value` by using **numeric keys** — the string is pasted into the SQL as-is:

```php
$users = $model->selectAll('users', null, [
    'status' => 'active',
    'email_verified != 0',           // no binding
    'deleted_at IS NULL',
]);
```

**Use when:** operators, functions, or literals with no user input.

---

### Step 4 — Choosing how to bind user input

Once a fragment needs a **value from PHP**, pick one of four shapes:

| Pattern | Example | Binds |
|--------|---------|--------|
| **A. Column shorthand** | `'id', 5` | one equality |
| **B. Associative key** | `'status' => 'active'` | one equality |
| **C. Direct `?` key** | `'created_at > ?' => $date` | **one** `?` in that key |
| **D. Placeholder in fragment + `$bindings`** | `'created_at > :date'` + `[':date' => $date]` | any `:name` in the SQL |
| **E. String WHERE + list** | `'a = ? AND b = ?', [$a, $b]` | each `?` left-to-right |
| **F. String WHERE + scalar** | `'created_at > ?', $date` | single `?` only |

```php
// C — one placeholder, value inline in the WHERE array (good for one `?` per condition)
$users = $model->selectAll('users', null, [
    'status' => 'active',
    'created_at > ?' => $date,
]);

// D — named placeholders in raw fragments; values in $bindings
$users = $model->selectAll('users', null, [
    'status' => 'active',
    'created_at > :date',
    'id IN (:ids)',
], [
    ':date' => $date,
    ':ids' => [1, 2, 3],
]);

// E — classic positional style
$users = $model->selectAll('users', null,
    'email = ? AND status = ?',
    ['john@example.com', 'active']
);
```

#### When to switch from C to D (named placeholders)

**Direct `?` keys (C)** tie **one PHP value to one `?` in that key**. They are ideal for a single comparison per array entry.

**Named placeholders (D)** are better when:

- **One fragment has several placeholders** — e.g. `'(score > :min AND score < :max)'` with `[':min' => 10, ':max' => 20]`
- **The same value appears more than once** — e.g. `'col1 > :cutoff OR col2 > :cutoff'` with `[':cutoff' => $x]` (bind once, reuse in SQL)
- **You use `IN (:ids)`** — array values expand to `:ids0`, `:ids1`, …
- **You mix named and positional** — e.g. `'status = :status AND id > ?'` with `[':status' => 'active', 5]`

```php
// ✅ Named — two placeholders in one grouped condition
$users = $model->selectAll('users', null, [
    'status' => 'active',
    '(last_login > :since OR created_at > :since)',
], [':since' => '2024-01-01']);

// ❌ Direct key cannot cleanly bind two values in one entry
// ['(a > ? OR b > ?)' => ???]  // only one value slot — use D or E instead
```

**Correction on a common assumption:** raw fragments like `'(col1 > :data OR col2 < :data2)'` do **not** auto-bind — you must supply every `:name` in `$bindings`. That is the intended pattern for multi-placeholder expressions.

---

### Step 5 — `$data` mirrors `$where` (INSERT / UPDATE SET)

The same key rules apply to **`$data`** for INSERT and UPDATE:

```php
$model->update('users', [
    'name' => 'John',                    // bound column
    'date_verified = NOW()',            // raw SQL (numeric key)
    'views = views + 1',                // raw SQL expression
    'last_login = ?' => $now,           // one `?` in SET
], 'id', 1);
```

---

### Step 6 — READ helpers

All SELECT paths go through the same builder; only the fetch differs:

```php
$stmt  = $model->select('users', ['id', 'name'], ['status' => 'active']); // PDOStatement
$row   = $model->selectOne(...);   // one associative row or []
$rows  = $model->selectAll(...);   // list of rows
```

Optional **`$sqlTail`**: `'ORDER BY name LIMIT 10'` appended after `WHERE`.

JOINs: put the join in `$table`, qualify columns in `$columns` / `$where`:

```php
$model->selectAll(
    'users U INNER JOIN profiles P ON P.user_id = U.id',
    ['U.name', 'P.bio'],
    ['U.status' => 'active']
);
```

---

### Step 7 — EXISTS

Same `$where` / `$bindings` as SELECT, returns `bool`:

```php
if ($model->exists('users', ['email' => $email, 'status' => 'active'])) { ... }
```

---

### Step 8 — INSERT (single row → batch)

```php
// Single row
$id = $model->insert('users', ['name' => 'John', 'created_at = NOW()']);

// Many rows — one INSERT per distinct column shape
$model->insert('users', [
    ['name' => 'A', 'email' => 'a@x.com'],
    ['name' => 'B', 'email' => 'b@x.com', 'role' => 'admin'],
]);

// Variants: insertReplace(), insertIgnore(), insertOnDuplicateKeyUpdate()
```

**Use batch INSERT** when inserting many rows with the same (or grouped) column sets. **Use `prepExec()`** for one-off SQL the helpers do not generate.

---

### Step 9 — UPDATE (single → batch)

**Single row** — classic `SET col = :set_col WHERE …`:

```php
$model->update('users', ['name' => 'John'], 'id', 1);
$model->update('users', ['name' => 'John'], ['id' => 1, 'status' => 'active']);
```

**Batch** — parallel lists for `$data`, `$where`, and optionally `$bindings` (same index = same logical row). One SQL statement with `CASE WHEN` per column:

```php
$since = '2024-01-01';
$teamIds = [10, 11, 12];

$data = [
    ['name' => 'Alice', 'views = views + 1'],     // row 0: raw SQL in SET
    ['name' => 'Carol'],                           // row 1: no $bindings entry
    ['name' => 'Bob', 'views = views + ?' => 3],  // row 2: `?` in SET key
    ['name' => 'Dave'],                            // row 3: scalar $bindings
];
$where = [
    ['id' => 1, 'email_verified != 0', 'created_at > :since', 'id IN (:ids)'],
    ['id' => 3],
    'id = ? AND role = ?',
    'id = 4 AND created_at > ?',
];
$bindings = [
    [':since' => $since, ':ids' => $teamIds],  // named map
    null,                                       // self-bound associative WHERE
    [99, 'editor'],                             // positional list
    $since,                                     // scalar → single `?` in string WHERE
];

$model->update('users', $data, $where, $bindings);
```

**When to use batch UPDATE:** many rows, each with its own SET + WHERE, in one round-trip. Row conditions should be **mutually exclusive** (usually `id = ?` per row) so `CASE WHEN` does not match the same DB row twice.

**When to stay on single UPDATE:** one row, or overlapping conditions, or when `prepExec()` is clearer.

---

### Step 10 — DELETE

Same `$where` / `$bindings` as SELECT; `$where` is required (no accidental full-table delete through an empty filter):

```php
$model->delete('users', 'id', 1);
$model->delete('users', ['status' => 'inactive', 'created_at > ?' => $cutoff]);
```

---

### Step 11 — `prepExec()` (escape hatch)

When helpers cannot express the SQL (CTEs, subqueries, multi-table UPDATE, etc.), run parameterized SQL directly:

```php
$stmt = $model->prepExec(
    'WITH recent AS (SELECT id FROM users WHERE created_at > ?) SELECT * FROM recent',
    ['2024-01-01']
);
```

Same `?` → named conversion and `IN (:ids)` expansion apply.

---

### Quick decision guide

```
One equality on one column?
  → column shorthand: update(..., 'id', 5)

Several AND equalities?
  → associative $where, omit $bindings

Fixed SQL, no user values?
  → numeric-key raw fragments

One user value, one comparison?
  → 'col > ?' => $value   OR   'col', $value

Several placeholders in one expression, or reuse same value?
  → raw fragment with :names + named $bindings map

Several ? in one string?
  → string WHERE + positional list (or named + map)

Many rows, same query shape?
  → INSERT: list of $data maps
  → UPDATE: parallel $data / $where / $bindings lists

SQL too custom?
  → prepExec()
```

See [Limitations](#limitations) for literal `?` in `LIKE` patterns and batch-update edge cases.

## Limitations

### Literal `?` Characters in Raw SQL

The `?` character is **always** treated as a placeholder and will be converted to a named placeholder, even when used as a literal character in SQL strings (e.g., in `LIKE` patterns).

**Problematic:**
```php
// ❌ The ? in LIKE pattern will be converted to a placeholder
$users = $model->selectAll('users', null, ["name LIKE '%Marc?'"]);
// The ? becomes :where_raw_0 without a binding — the query returns no rows

// ❌ Also problematic in string WHERE clauses
$users = $model->selectAll('users', null, "name LIKE '%Marc?' AND status = ?", ['active']);
// Both ? characters are converted, but only one binding is provided — PDO error or no match
```

**Solutions:**
```php
// ✅ Put the pattern with literal ? in the bindings array, not in the SQL string
$pattern = '%Marc?';
$users = $model->selectAll('users', null, "name LIKE ? AND status = ?", 
    [$pattern, 'active']);
// The trait converts both ? to named placeholders, and the literal ? stays in the binding value

// ✅ Same approach for raw SQL in arrays
$pattern = '%Marc?';
$users = $model->selectAll('users', null, ["name LIKE ?", "status = ?"], 
    [$pattern, 'active']);
```

**Note:** Named placeholders (`:name`) are only matched when they exist in the bindings array and use word boundaries, so literal `:` characters in raw SQL strings are safe. Only `?` characters have this limitation.

### Batch UPDATE

- **Overlapping row conditions** — each column uses `CASE WHEN …`; if one DB row matches more than one batch entry's WHERE, only the **first** matching branch applies. Prefer unique keys per row (e.g. `id`).
- **Per-row `$bindings`** — parallel lists must align with `$data` / `$where` by index; use `null` when a row needs no external bindings (not `[null]`).
- **Same `:name`, different values per row** — in one batch statement, a named placeholder has **one** binding; reuse the name only when the value is shared across rows.
- **Large batches** — very wide `CASE` statements may hit packet or planner limits; chunk if needed.

## Testing

Each documented code example in the trait PHPDoc and this README has a matching test in
`tests/Integration/CommonModelPicoPdoTraitDocExamplesTest.php` (integration) and, for SQL helpers,
`tests/Unit/CommonModelPicoPdoTraitTest.php` (unit). Test names are prefixed with `testDoc`.

Current test coverage (`make test` prints a text summary; `make test-coverage` generates HTML):
- **217 tests** (87 unit, 130 integration)
- **522 assertions**
- **100%** lines and methods on `CommonModelPicoPdoTrait` (Xdebug)

```bash
make test              # Run all tests
make test-coverage     # Generate HTML coverage report
```

## License

Property of Lodur
