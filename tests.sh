#!/bin/bash
set -e

php ./tests/loadS3.php
./vendor/bin/phpunit
