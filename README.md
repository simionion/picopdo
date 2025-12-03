# PicoPdo

A lightweight PDO trait for PHP models that provides common database operations with automatic placeholder conversion and flexible query building.

## Features

- **Automatic `?` to named placeholder conversion** - All positional placeholders are converted to named placeholders
- **Array-expansion for `IN` clauses** - Supports both `IN (:ids)` and `IN (?)` syntax
- **Flexible WHERE clause building** - Arrays, raw SQL, mixed placeholders all supported
- **Raw SQL in arrays** - Use numeric keys for raw SQL strings
- **Keys with `?` placeholders** - Direct binding syntax: `['date > ?' => $date]`
- **INSERT modes** - INSERT, REPLACE, INSERT IGNORE, ON DUPLICATE KEY UPDATE
- **JOIN support** - `selectJoin()` method for complex queries
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
    'date > ?' => '2024-01-01'    // Key contains ?, value is binding
]);
// Generates: WHERE `status` = :where_status AND date > :where_0
```

**Mixed placeholders:**
```php
$users = $model->selectAll('users', null,
    'status = :status AND id > ?',
    [':status' => 'active', 5]
);
// Generates: WHERE status = :status AND id > :where_0
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

### 5. JOIN Support

```php
$stmt = $model->selectJoin(
    'users u',
    ['u.id', 'u.name', 'p.bio'],
    'LEFT JOIN profiles p ON p.user_id = u.id',
    ['u.id' => 1]
);

// Multiple JOINs
$joins = [
    'LEFT JOIN profiles p ON p.user_id = u.id',
    'LEFT JOIN addresses a ON a.user_id = u.id'
];
$stmt = $model->selectJoin('users u', ['u.*', 'p.bio', 'a.address'], $joins, 'u.status = ?', ['active']);
```

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

// With raw SQL
$affected = $model->update('users', 
    ['name' => 'John', 'status = "inactive"'],  // Raw SQL for status
    'id', 1
);
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

Here's a comprehensive example showcasing all features together:

```php
// Complex query with JOINs, flexible WHERE, IN clauses, raw SQL, and sqlTail with bindings
$stmt = $model->selectJoin(
    'users u',
    [
        'u.id',
        'u.name',
        'u.email',
        'p.bio',
        'COUNT(o.id) AS order_count',
        'SUM(o.total) AS total_spent'
    ],
    [
        'LEFT JOIN profiles p ON p.user_id = u.id',
        'LEFT JOIN orders o ON o.user_id = u.id'
    ],
    [
        'u.status' => 'active',                    // Key with dot - sanitized to :where_u_status
        'u.name != ""',                            // Raw SQL (numeric key)
        'u.created_at > :min_date',                 // Raw SQL with named placeholder
        'u.created_at < ?' => $maxLastLogin,        // Key with ? placeholder
        'u.id IN (:user_ids)'                       // IN clause with named placeholder (raw SQL)
    ],
    [
        ':min_date' => '2024-01-01',
        ':user_ids' => [1, 2, 3],                   // Will expand to :user_ids0, :user_ids1, :user_ids2
        ':min_spent' => 1000                        // For HAVING clause in sqlTail
    ],
    'GROUP BY u.id HAVING total_spent > :min_spent ORDER BY total_spent DESC LIMIT 10'
);

$results = $stmt->fetchAll(PDO::FETCH_ASSOC);
```

## Limitations

### Literal `?` Characters in Raw SQL

The `?` character is **always** treated as a placeholder and will be converted to a named placeholder, even when used as a literal character in SQL strings (e.g., in `LIKE` patterns).

**Problematic:**
```php
// ❌ The ? in LIKE pattern will be converted to a placeholder
$users = $model->selectAll('users', null, ["name LIKE '%Marc?'"]);
// The ? becomes :where_raw_0, causing a binding mismatch

// ❌ Also problematic in string WHERE clauses
$users = $model->selectAll('users', null, "name LIKE '%Marc?' AND status = ?", ['active']);
// Both ? characters are converted, but only one binding is provided
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

## Testing

Current test coverage:
- **196 tests** (135 unit tests, 61 integration tests)
- **596 assertions**
- Classes: 100.00% (1/1)
- Methods: 100.00% (17/17)
- Lines: 100.00% (133/133)

```bash
make test              # Run all tests
make test-coverage     # Generate HTML coverage report
```

## License

Property of Lodur
