.PHONY: build up down test clean install

# Build and start the containers
build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

# Run tests
test:
	docker-compose exec app vendor/bin/phpunit --coverage-text

# Install dependencies
install:
	docker-compose exec app composer install

# Clean up
clean:
	docker-compose down -v
	rm -rf vendor/
	rm -rf .phpunit.cache/

# Quick start (build, install, up)
start: build install up

# Run tests with HTML coverage report
test-coverage:
	docker-compose exec app vendor/bin/phpunit --coverage-html coverage

# Show logs
logs:
	docker-compose logs -f 