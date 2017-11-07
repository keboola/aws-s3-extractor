# Keboola AWS S3 Extractor
 
[![Build Status](https://travis-ci.org/keboola/aws-s3-extractor.svg?branch=master)](https://travis-ci.org/keboola/aws-s3-extractor) [![Code Climate](https://codeclimate.com/github/keboola/aws-s3-extractor/badges/gpa.svg)](https://codeclimate.com/github/keboola/aws-s3-extractor) [![Test Coverage](https://codeclimate.com/github/keboola/aws-s3-extractor/badges/coverage.svg)](https://codeclimate.com/github/keboola/aws-s3-extractor)

Download files from S3 to `/data/out/files`. 

## Features
- Use `*` or `%` for wildcards
- Subfolders
- Process only new files

## Configuration options

- `accessKeyId` (required) -- AWS Access Key ID
- `#secretAccessKey` (required) -- AWS Secret Access Key
- `bucket` (required) -- AWS S3 bucket name, it's region will be autodetected
- `key` (required) -- Search key prefix, optionally ending with a `*` wildcard. all filed downloaded with a wildcard are stored in `/data/out/files/wildcard` folder.
- `includeSubfolders` (optional) -- Download also all subfolders, only available with a wildcard in the search key prefix. 
Subfolder structure will be flattened, `/` in the path will be replaced with a `-` character, eg `folder1/file1.csv => folder1-file1.csv`. 
Existing `-` characters will be escaped with an extra `-` character to resolve possible collisions, eg. `collision-file.csv => collision--file.csv`.  
- `newFilesOnly` (optional) -- Download only new files. Last file timestamp is stored in the `lastDownloadedFileTimestamp` property of the state file.  

### Sample configurations

#### Single file

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

#### Wildcard 

```json
{
    "parameters": {
        "accessKeyId": "AKIA****",
        "#secretAccessKey": "****",
        "bucket": "myBucket",
        "key": "myfolder/*",
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
docker-compose run --rm tests
```

