# Keboola AWS S3 Extractor

This component downloads files from S3 to `/data/out/files`.

## Features
- Supports `*` wildcards
- Handles subfolders
- Can process only new files
- Skips files stored in Glacier & Glacier Deep Archive

## Configuration Options

- `loginType` (required) -- Login type (`credentials` or `role`)
- `accessKeyId` (required if `loginType` is `"credentials"`) -- AWS Access Key ID
- `#secretAccessKey` (required if `loginType` is `"credentials"`) -- AWS Secret Access Key
- `accountId` (required if loginType is "role") - AWS Account ID
- `bucket` (required) -- AWS S3 bucket name (the region will be autodetected)
- `key` (required) -- Search key prefix, optionally ending with a `*` wildcard. All filed downloaded using a wildcard are stored in `/data/out/files/wildcard`.
- `saveAs` (optional) -- Store all downloaded files in a specified folder.
- `includeSubfolders` (optional) -- Download all subfolders. Available only when using a wildcard in the search key prefix.
    - The subfolder structure will be flattened, replacing `/` in the path with `-`, e.g., `folder1/file1.csv` => `folder1-file1.csv`.
    - Existing `-` characters will be escaped to avoid colisions with another `-` , e.g., `collision-file.csv` => `collision--file.csv`.
- `newFilesOnly` (optional) -- Download only new files.
    - The last downloaded file's timestamp is stored in the `lastDownloadedFileTimestamp` property of the state file.
    - If multiple files have the same timestamp, `processedFilesInLastTimestampSecond` records all processed files within that second.
- `limit` (optional, default `0`) -- Maximum number of files to download.
    - If `key` matches more files than `limit`, the oldest files will be downloaded first.
    - When used with `newFilesOnly`, the extractor will process up to `limit` new files that have not been downloaded yet.

### Sample Configurations

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

#### Wildcard, subfolders, and new files only

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

***Note:** state.json must be provided in this case.*

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

***Note:** state.json has to be provided in this case.*

## Development

### Preparation

- Create an AWS S3 bucket and IAM user using [`aws-services.json`](./aws-services.json) CloudFormation template.
- Create a `.env` file. Use the output of the `aws-services` CloudFront stack to populate the variables, along with your Redshift credentials.

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
KBC_PROJECTID=
KBC_STACKID=
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
Run tests with the following command.

```
docker-compose run --rm dev ./vendor/bin/phpunit
```


## License

MIT licensed, see the [LICENSE](./LICENSE) file.
