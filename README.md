# Keboola AWS S3 Extractor

[![Build Status](https://travis-ci.org/keboola/aws-s3-extractor.svg?branch=master)](https://travis-ci.org/keboola/aws-s3-extractor) [![Code Climate](https://codeclimate.com/github/keboola/aws-s3-extractor/badges/gpa.svg)](https://codeclimate.com/github/keboola/aws-s3-extractor) [![Test Coverage](https://codeclimate.com/github/keboola/aws-s3-extractor/badges/coverage.svg)](https://codeclimate.com/github/keboola/aws-s3-extractor)

Download files from S3 to `/data/out/files`.

## Features
- Use `*` for wildcards
- Subfolders
- Can process only new files
- Skips files stored in Glacier

## Configuration options

- `loginType` (required) -- Login type (credentials/role)
- `accessKeyId` (required if your choise loginType "credentials") -- AWS Access Key ID
- `#secretAccessKey` (required if your choise loginType "credentials") -- AWS Secret Access Key
- `accountId` (required if your choise loginType "role") - AWS Account Id
- `bucket` (required) -- AWS S3 bucket name, it's region will be autodetected
- `key` (required) -- Search key prefix, optionally ending with a `*` wildcard. all filed downloaded with a wildcard are stored in `/data/out/files/wildcard` folder.
- `saveAs` (optional) -- Store all downloaded file(s) in a folder.
- `includeSubfolders` (optional) -- Download also all subfolders, only available with a wildcard in the search key prefix.
Subfolder structure will be flattened, `/` in the path will be replaced with a `-` character, eg `folder1/file1.csv => folder1-file1.csv`.
Existing `-` characters will be escaped with an extra `-` character to resolve possible collisions, eg. `collision-file.csv => collision--file.csv`.
- `newFilesOnly` (optional) -- Download only new files. Last file timestamp is stored in the `lastDownloadedFileTimestamp` property of the state file.
If more files with the same timestamp exist, the state `processedFilesInLastTimestampSecond` property is used to save all processed files within the given second.
- `limit` (optional, default `0`) -- Maximum number of files downloaded, if the `key` matches more files than `limit`, the oldest files will be downloaded.
If used together with `newFilesOnly`, the extractor will process `limit` number of files that have not yet been processed.

### Sample configurations

#### Single file (login via credentials)

```json
{
    "parameters": {
        "accessKeyId": "AKIA****",
        "#secretAccessKey": "****",
        "bucket": "myBucket",
        "key": "myfile.csv",
        "includeSubfolders": false,
        "newFilesOnly": false
    }
}
```

#### Single file (login via role)
```json
{
    "parameters": {
        "accountId": "1234567890",
        "bucket": "myBucket",
        "key": "myfile.csv",
        "includeSubfolders": false,
        "newFilesOnly": false
    }
}
```

#### Wildcard

```json
{
    "parameters": {
        "accessKeyId": "AKIA****",
        "#secretAccessKey": "****",
        "bucket": "myBucket",
        "key": "myfolder/*",
        "saveAs": "myfolder",
        "includeSubfolders": false,
        "newFilesOnly": false
    }
}
```

#### Wildcard, subfolders and new files only

```json
{
    "parameters": {
        "accessKeyId": "AKIA****",
        "#secretAccessKey":  "****",
        "bucket": "myBucket",
        "key": "myfolder/*",
        "includeSubfolders": true,
        "newFilesOnly": true
    }
}
```

*Note: state.json has to be provided in this case*

#### Small increments, suitable for frequent jobs

```json
{
    "parameters": {
        "accessKeyId": "AKIA****",
        "#secretAccessKey":  "****",
        "bucket": "myBucket",
        "key": "myfolder/*",
        "includeSubfolders": true,
        "newFilesOnly": true,
        "limit": 100
    }
}
```

*Note: state.json has to be provided in this case*

## Development

### Preparation

- Create AWS S3 bucket and IAM user using [`aws-services.json`](./aws-services.json) CloudFormation template.
- Create `.env` file. Use output of `aws-services` CloudFront stack to fill the variables and your Redshift credentials.

```
AWS_S3_BUCKET=
AWS_REGION=
UPLOAD_USER_AWS_ACCESS_KEY=
UPLOAD_USER_AWS_SECRET_KEY=
DOWNLOAD_USER_AWS_ACCESS_KEY=
DOWNLOAD_USER_AWS_SECRET_KEY=
KEBOOLA_USER_AWS_ACCESS_KEY=
KEBOOLA_USER_AWS_SECRET_KEY=
ACCOUNT_ID=
ROLE_NAME=
EXTERNAL_ID=
KBC_STACK_ID=
```

- Build Docker images
```
docker-compose build
```

- Install Composer packages

```
docker-compose run --rm dev composer install --prefer-dist --no-interaction
```

### Tests Execution
Run tests with following command.

```
docker-compose run --rm dev ./vendor/bin/phpunit
```

