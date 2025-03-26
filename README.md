# PicoPdo

A lightweight PDO trait for PHP models that provides common database operations.

## Features

- Secure, parameterized queries with array-expansion for `IN` clauses
- Simplified methods for SELECT, INSERT, UPDATE, DELETE, and EXISTS operations
- Automatically binds parameters and handles execution errors
- Reduces duplication and boilerplate in model implementations
- 100% test coverage across unit and integration tests

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
- `make test` - Run PHPUnit tests with coverage report
- `make clean` - Clean up Docker volumes and vendor directory
- `make install` - Install dependencies
- `make logs` - Show container logs

## Testing

The project includes both unit and integration tests with 100% code coverage:

- Unit tests (`tests/Unit/`) - Test individual methods using mocks
- Integration tests (`tests/Integration/`) - Test against a real database

Current test coverage:
- Classes: 100% (1/1)
- Methods: 100% (9/9)
- Lines: 100% (71/71)

To run specific test suites:
```bash
# Run unit tests only
vendor/bin/phpunit --testsuite Unit

# Run integration tests only
vendor/bin/phpunit --testsuite Integration
```

## Usage

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

// Example usage
$userModel = new UserModel($pdo);

// Basic CRUD operations
$id = $userModel->insert('users', [
    'name' => 'John Doe',
    'email' => 'john@example.com',
    'status' => 'active'
]);
// Generates: INSERT INTO users (`name`,`email`,`status`) VALUES (?,?,?)
// Parameters: ['John Doe', 'john@example.com', 'active']

// Select a single record by ID
$user = $userModel->select('users', '*', 'user_id', 123);
// Generates: SELECT * FROM `users` WHERE `user_id` = :where_user_id LIMIT 1
// Parameters: [':where_user_id' => 123]

// Select a single record with simple conditions
$user = $userModel->select('users', ['name', 'email'], [
    'id' => 1,
    'status' => 'active'
]);
// Generates: SELECT name, email FROM `users` WHERE `id` = :where_id AND `status` = :where_status LIMIT 1
// Parameters: [':where_id' => 1, ':where_status' => 'active']

// Select all records with IN clause
$users = $userModel->selectAll('users', ['name', 'email'],
    'status = :status AND role IN (:roles)',
    [
        ':status' => 'active',
        ':roles' => ['admin', 'moderator']
    ]
);
// Generates: SELECT name, email FROM `users` WHERE status = :status AND role IN (:roles0,:roles1)
// Parameters: [':status' => 'active', ':roles0' => 'admin', ':roles1' => 'moderator']

// Update records with complex conditions
$userModel->update('users', 
    ['status' => 'inactive'],
    'role IN (:roles) AND created_at < NOW()',
    [':roles' => ['admin', 'moderator']]
);
// Generates: UPDATE `users` SET `status` = :set_status WHERE role IN (:roles0,:roles1) AND created_at < NOW()
// Parameters: [':set_status' => 'inactive', ':roles0' => 'admin', ':roles1' => 'moderator']

// Delete records with complex conditions
$userModel->delete('users',
    'status = :status AND last_login < DATE_SUB(NOW(), INTERVAL :days DAY)',
    [':status' => 'inactive', ':days' => 30]
);
// Generates: DELETE FROM `users` WHERE status = :status AND last_login < DATE_SUB(NOW(), INTERVAL :days DAY)
// Parameters: [':status' => 'inactive', ':days' => 30]

// Check if record exists with simple conditions
$exists = $userModel->exists('users', [
    'email' => 'john@example.com',
    'status' => 'active'
]);
// Generates: SELECT 1 as `true` FROM `users` WHERE `email` = :where_email AND `status` = :where_status LIMIT 1
// Parameters: [':where_email' => 'john@example.com', ':where_status' => 'active']

// Select with complex conditions using named placeholders
$users = $userModel->selectAll('users', ['name', 'email'],
    'status = :status AND role IN (:roles) AND created_at < NOW() AND email LIKE :email AND age > :age',
    [
        ':status' => 'active',
        ':roles' => ['admin', 'moderator'],
        ':email' => '%@example.com',
        ':age' => 18
    ]
);
// Generates: SELECT name, email FROM `users` WHERE status = :status AND role IN (:roles0,:roles1) AND created_at < NOW() AND email LIKE :email AND age > :age
// Parameters: [':status' => 'active', ':roles0' => 'admin', ':roles1' => 'moderator', ':email' => '%@example.com', ':age' => 18]

// For complex queries, use prepExec directly
// prepExec combines prepare & execute in one call, with special support for IN clauses
// When a parameter contains an array, it's automatically expanded into multiple placeholders
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

// Example of prepExec with IN clause - array values are automatically expanded
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

## License

Property of Lodur 