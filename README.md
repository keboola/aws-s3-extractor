# Keboola AWS S3 Extractor
 
[![Build Status](https://travis-ci.org/keboola/aws-s3-extractor.svg?branch=master)](https://travis-ci.org/keboola/aws-s3-extractor) [![Code Climate](https://codeclimate.com/github/keboola/aws-s3-extractor/badges/gpa.svg)](https://codeclimate.com/github/keboola/aws-s3-extractor) [![Test Coverage](https://codeclimate.com/github/keboola/aws-s3-extractor/badges/coverage.svg)](https://codeclimate.com/github/keboola/aws-s3-extractor)

Download files from S3 to `/data/out/files`. 

## Features
- Wildcard - use `*` or `%` at the end for wildcards; wildcards do not download subfolders.

## Development

### Preparation

- Create AWS S3 bucket and IAM user using [`aws-services.json`](./aws-services.json) CloudFormation template.
- Create `.env` file. Use output of `aws-services` CloudFront stack to fill the variables and your Redshift credentials.
```
AWS_S3_BUCKET=
AWS_REGION=
PREPARE_TESTS_AWS_ACCESS_KEY=
PREPARE_TESTS_AWS_SECRET_KEY=
TESTS_AWS_ACCESS_KEY=
TESTS_AWS_SECRET_KEY=
```

- Build Docker images
```
docker-compose build
```

- Install Composer packages

```
docker-compose run --rm dev composer install --prefer-dist --no-interaction
```

Upload test fixtures to S3:
```
docker-compose run dev php ./tests/loadS3.php
```

### Tests Execution
Run tests with following command.

```
docker-compose run --rm tests
```

Tests are executed against real S3. S3 credentials have to be provided.
