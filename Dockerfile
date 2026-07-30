FROM php:8.3-cli

# Install system dependencies (retry apt update — mirrors can flake)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
        unzip \
        $PHPIZE_DEPS \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install PHP extensions used by the project / Xdebug
RUN docker-php-ext-install pdo_mysql \
    && pecl install xdebug \
    && docker-php-ext-enable xdebug \
    && echo "xdebug.mode=coverage" >> /usr/local/etc/php/conf.d/docker-php-ext-xdebug.ini

# Install Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/html

COPY . .

# Prefer lockfile install; fall back if lock is out of sync in CI builds
RUN composer install --no-interaction --prefer-dist || composer update --no-interaction --prefer-dist

RUN chown -R www-data:www-data /var/www/html
