# PicoPdo

A lightweight PDO trait for PHP models that provides common database operations with automatic placeholder conversion and flexible query building.

## Features

- **Secure, parameterized queries** with automatic `?` to named placeholder conversion
- **Array-expansion for `IN` clauses** - supports both `IN (:ids)` and `IN (?)` syntax
- **Flexible WHERE clause building** - supports arrays, raw SQL, mixed placeholders
- **Simplified methods** for SELECT, INSERT, UPDATE, DELETE, and EXISTS operations
- **INSERT modes** - INSERT, REPLACE, INSERT IGNORE, ON DUPLICATE KEY UPDATE
- **Automatic parameter binding** and error handling
- **100% test coverage** across unit and integration tests

## Requirements

- PHP 8.2 or higher
- PDO PHP extension
- MariaDB/MySQL
- Xdebug (for code coverage reports)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/simionion/picopdo.git
cd picopdo
```

2. Install dependencies:
```bash
make install
```

## Development Setup

1. Build and start the Docker containers:
```bash
make start
```

2. Run tests with coverage report:
```bash
make test
```

## Available Make Commands

- `make build` - Build Docker containers
- `make up` - Start containers
- `make down` - Stop containers
- `make start` - Build, start containers, and install dependencies
- `make test` - Run PHPUnit tests with text coverage report
- `make test-coverage` - Run PHPUnit tests with HTML coverage report (generates `coverage/` directory)
- `make install` - Install Composer dependencies
- `make clean` - Clean up Docker volumes and vendor directory
- `make logs` - Show container logs

## Testing

The project includes both unit and integration tests with comprehensive code coverage:

- **Unit tests** (`tests/Unit/`) - Test individual methods using mocks
- **Integration tests** (`tests/Integration/`) - Test against a real database

Current test coverage:
- Classes: 100.00% (1/1)
- Methods: 100.00% (16/16)
- Lines: 100.00% (125/125)

To run specific test suites:
```bash
# Run unit tests only
docker-compose exec app vendor/bin/phpunit --testsuite Unit

# Run integration tests only
docker-compose exec app vendor/bin/phpunit --testsuite Integration

# Run tests with HTML coverage report
make test-coverage
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

$userModel = new UserModel($pdo);
```

### INSERT Operations

#### Basic Insert
```php
$id = $userModel->insert('users', [
    'name' => 'John Doe',
    'email' => 'john@example.com',
    'status' => 'active'
]);
// Returns: Inserted record ID or 0 if failed
```

#### Insert with Raw SQL
```php
$id = $userModel->insert('users', [
    'name' => 'John Doe',
    'created_at = NOW()',  // Raw SQL
    'uuid = UUID()'        // Raw SQL
]);
```

#### Insert with Meta Information
```php
$result = $userModel->insert('users', [
    'name' => 'John Doe',
    'email' => 'john@example.com'
], ['meta' => true]);
// Returns: ['id' => 123, 'rows' => 1, 'status' => 'inserted']
```

#### REPLACE INTO
```php
$id = $userModel->insertReplace('users', [
    'name' => 'John Doe',
    'email' => 'john@example.com'
]);
```

#### INSERT IGNORE
```php
$result = $userModel->insertIgnore('users', [
    'name' => 'John Doe',
    'email' => 'john@example.com'
]);
// Returns: ['id' => 123|0, 'rows' => 1|0, 'status' => 'inserted'|'noop']
```

#### INSERT ... ON DUPLICATE KEY UPDATE
```php
$result = $userModel->insertOnDuplicateKeyUpdate('users', 
    ['name' => 'John Doe', 'email' => 'john@example.com'],
    ['name' => 'John Doe', 'status' => 'active']  // Update values
);
// Returns: ['id' => 123|0, 'rows' => 1|2|0, 'status' => 'inserted'|'updated'|'noop']
```

### SELECT Operations

#### Select One Record (Classic Key-Value)
```php
$user = $userModel->selectOne('users', ['name', 'email'], 'id', 1);
// Generates: SELECT name, email FROM users WHERE `id` = :where_id LIMIT 1
```

#### Select One Record (Associative Array)
```php
$user = $userModel->selectOne('users', ['name', 'email'], [
    'id' => 1,
    'status' => 'active'
]);
// Generates: SELECT name, email FROM users WHERE `id` = :where_id AND `status` = :where_status LIMIT 1
```

#### Select All Records (Simple Conditions)
```php
$users = $userModel->selectAll('users', ['name', 'email'], [
    'status' => 'active',
    'email_verified' => 1
]);
```

#### Select with Raw SQL in Array
```php
$users = $userModel->selectAll('users', ['name', 'email'], [
    'status' => 'active',
    'email_verified != 0',           // Raw SQL (numeric key)
    'created_at > :date'              // Raw SQL with named placeholder
], [':date' => '2024-01-01']);
// Generates: SELECT name, email FROM users WHERE `status` = :where_status AND email_verified != 0 AND created_at > :date
```

#### Select with `?` Placeholders (Auto-converted to Named)
```php
$users = $userModel->selectAll('users', ['name', 'email'], 
    'email = ? AND created_at > ?',
    ['user@example.com', '2024-01-01']
);
// Generates: SELECT name, email FROM users WHERE email = :where_0 AND created_at > :where_1
// Parameters: [':where_0' => 'user@example.com', ':where_1' => '2024-01-01']
```

#### Select with Mixed Placeholders
```php
$users = $userModel->selectAll('users', ['name', 'email'],
    'status = :status AND id > ?',
    [':status' => 'active', 5]
);
// Generates: SELECT name, email FROM users WHERE status = :status AND id > :where_0
```

#### Select with `IN (:ids)` (Named Placeholder)
```php
$users = $userModel->selectAll('users', ['name', 'email'],
    'id IN (:ids)',
    [':ids' => [1, 2, 3]]
);
// Generates: SELECT name, email FROM users WHERE id IN (:ids0, :ids1, :ids2)
// Parameters: [':ids0' => 1, ':ids1' => 2, ':ids2' => 3]
```

#### Select with `IN (?)` (Positional Placeholder - New Feature!)
```php
$users = $userModel->selectAll('users', ['name', 'email'],
    'id IN (?)',
    [[1, 2, 3]]  // Note: Array wrapped in array
);
// Generates: SELECT name, email FROM users WHERE id IN (:where_00, :where_01, :where_02)
// Parameters: [':where_00' => 1, ':where_01' => 2, ':where_02' => 3]
```

#### Select with Key Containing `?` Placeholder
```php
$users = $userModel->selectAll('users', ['name', 'email'], [
    'status' => 'active',
    'date > ?' => '2024-01-01'  // Key with ? placeholder
]);
// Generates: SELECT name, email FROM users WHERE `status` = :where_status AND date > :where_0
```

#### Select with Complex Conditions
```php
$users = $userModel->selectAll('users', ['name', 'email'],
    'status = :status AND role IN (:roles) AND created_at < NOW() AND email LIKE :email AND age > :age',
    [
        ':status' => 'active',
        ':roles' => ['admin', 'moderator'],
        ':email' => '%@example.com',
        ':age' => 18
    ]
);
// Generates: SELECT name, email FROM users WHERE status = :status AND role IN (:roles0,:roles1) AND created_at < NOW() AND email LIKE :email AND age > :age
```

#### Select with Extra Query Suffix
```php
$users = $userModel->selectAll('users', ['name'],
    ['status' => 'active'],
    null,
    'ORDER BY name ASC LIMIT 10'
);
```

### UPDATE Operations

#### Basic Update
```php
$affected = $userModel->update('users', 
    ['name' => 'John Doe'],
    'id',
    1
);
// Returns: Number of affected rows
```

#### Update with Raw SQL in Data
```php
$affected = $userModel->update('users', 
    ['name' => 'John', 'status = "inactive"'],  // Raw SQL for status
    'id',
    1
);
```

#### Update with Array WHERE
```php
$affected = $userModel->update('users',
    ['status' => 'inactive'],
    ['id' => 1, 'active' => 1]
);
```

#### Update with Raw SQL in WHERE Array
```php
$affected = $userModel->update('users',
    ['status' => 'active'],
    ['id' => 1, 'email_verified != 0', 'created_at > :date'],
    [':date' => '2024-01-01']
);
```

#### Update with `?` Placeholders
```php
$affected = $userModel->update('users',
    ['name' => 'John'],
    'email = ? OR status = ?',
    ['john@example.com', 'active']
);
// Generates: UPDATE users SET `name` = :set_name WHERE email = :where_0 OR status = :where_1
```

#### Update with `IN (?)` Syntax
```php
$affected = $userModel->update('users',
    ['status' => 'inactive'],
    'id IN (?)',
    [[1, 2, 3]]
);
```

### DELETE Operations

#### Basic Delete
```php
$affected = $userModel->delete('users', 'id', 1);
```

#### Delete with Array WHERE
```php
$affected = $userModel->delete('users', [
    'status' => 'inactive',
    'email_verified' => 0
]);
```

#### Delete with Raw SQL in WHERE
```php
$affected = $userModel->delete('users',
    ['status' => 'inactive', 'email_verified != 0', 'created_at > :date'],
    [':date' => '2024-01-01']
);
```

#### Delete with `?` Placeholders
```php
$affected = $userModel->delete('users',
    'last_login < ? AND status != ?',
    ['2023-01-01', 'active']
);
// Generates: DELETE FROM users WHERE last_login < :where_0 AND status != :where_1
```

#### Delete with `IN (:ids)`
```php
$affected = $userModel->delete('users',
    'id IN (:ids)',
    [':ids' => [1, 2, 3]]
);
```

### EXISTS Operations

#### Basic Exists
```php
$exists = $userModel->exists('users', 'id', 1);
```

#### Exists with Array WHERE
```php
$exists = $userModel->exists('users', [
    'email' => 'john@example.com',
    'status' => 'active'
]);
```

#### Exists with Raw SQL
```php
$exists = $userModel->exists('users',
    ['status' => 'active', 'email_verified != 0', 'created_at > :date'],
    [':date' => '2024-01-01']
);
```

#### Exists with `?` Placeholders
```php
$exists = $userModel->exists('users',
    'email = ? AND created_at > ?',
    ['user@example.com', '2024-01-01']
);
```

#### Exists with `IN (?)`
```php
$exists = $userModel->exists('users',
    'id IN (?)',
    [[1, 2, 3]]
);
```

### Advanced: Direct SQL Execution

For complex queries, use `prepExec()` directly:

```php
// Simple query
$stmt = $userModel->prepExec(
    'SELECT u.name, u.email, o.order_id, o.total 
     FROM users u 
     JOIN orders o ON u.id = o.user_id 
     WHERE u.status = :status AND o.created_at > :date',
    [
        ':status' => 'active',
        ':date' => '2024-01-01'
    ]
);
$results = $stmt->fetchAll(PDO::FETCH_ASSOC);
```

#### prepExec with IN Clause (Named Placeholder)
```php
$stmt = $userModel->prepExec(
    'SELECT u.name, COUNT(o.order_id) as order_count 
     FROM users u 
     LEFT JOIN orders o ON u.id = o.user_id 
     WHERE u.role IN (:roles) AND o.status = :status 
     GROUP BY u.id',
    [
        ':roles' => ['admin', 'moderator'],  // Will be expanded to :roles0,:roles1
        ':status' => 'completed'
    ]
);
$results = $stmt->fetchAll(PDO::FETCH_ASSOC);
```

## Key Features Explained

### Automatic Placeholder Conversion

All `?` placeholders are automatically converted to named placeholders for consistency and better debugging:

- `?` → `:where_0`, `:where_1`, etc. (in WHERE clauses)
- `?` → `:set_0`, `:set_1`, etc. (in SET clauses)
- `?` → `:where_raw_0`, `:where_raw_1`, etc. (in raw SQL strings within arrays)

### Array Format Rules

When using arrays for WHERE clauses or data:

- **Numeric keys**: Treated as raw SQL strings
  ```php
  ['status' => 'active', 'email_verified != 0']  // 'email_verified != 0' is raw SQL
  ```

- **String keys with `?`**: Treated as SQL with placeholder
  ```php
  ['date > ?' => '2024-01-01']  // Key contains ?, value is binding
  ```

- **String keys without `?`**: Treated as column-value pairs
  ```php
  ['status' => 'active']  // Column = value
  ```

- **Keys starting with `:`**: Ignored (placeholders should be in `$bindings` parameter)

### IN Clause Support

Two syntaxes are supported for `IN` clauses:

1. **Named placeholder** (original):
   ```php
   'id IN (:ids)', [':ids' => [1, 2, 3]]
   // Expands to: id IN (:ids0, :ids1, :ids2)
   ```

2. **Positional placeholder** (new):
   ```php
   'id IN (?)', [[1, 2, 3]]  // Note: Array wrapped in array
   // Expands to: id IN (:where_00, :where_01, :where_02)
   ```

## License

Property of Lodur
