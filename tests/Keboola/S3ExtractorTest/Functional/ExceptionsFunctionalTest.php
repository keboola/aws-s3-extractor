<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Application;
use Keboola\S3Extractor\Exception;
use Monolog\Handler\TestHandler;

class ExceptionsFunctionalTest extends FunctionalTestCase
{
    public function testInvalidBucket()
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage("404 Not Found (NoSuchBucket)");
        $this->expectExceptionMessage("The specified bucket does not exist");
        $this->expectExceptionMessage(getenv(self::AWS_S3_BUCKET_ENV) . "_invalid");
        $application = new Application(
            [
                "parameters" => [
                    "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    "bucket" => getenv(self::AWS_S3_BUCKET_ENV) . "_invalid",
                    "key" => "/file1.csv",
                    "includeSubfolders" => false,
                    "newFilesOnly" => false,
                    "limit" => 0
                ],
            ],
            [],
            new TestHandler()
        );
        $application->actionRun($this->path);
    }

    public function testInvalidCredentials()
    {

        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Invalid credentials or permissions.");

        $application = new Application(
            [
                "parameters" => [
                    "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV) . "_invalid",
                    "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                    "key" => "/file1.csv",
                    "includeSubfolders" => false,
                    "newFilesOnly" => false,
                    "limit" => 0
                ],
            ],
            [],
            new TestHandler()
        );
        $application->actionRun($this->path);
    }

    public function testInvalidKey()
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage("404 Not Found (NotFound)");

        $application = new Application(
            [
                "parameters" => [
                    "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                    "key" => "/doesnotexist",
                    "includeSubfolders" => false,
                    "newFilesOnly" => false,
                    "limit" => 0
                ],
            ],
            [],
            new TestHandler()
        );
        $application->actionRun($this->path);
    }

    public function testMissingWildcardOrPathFile()
    {
        $application = new Application(
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'folder3/',
                ],
            ],
            [],
            new TestHandler()
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Use the wildcard flag or enter a full path to the file.');

        $application->actionRun($this->path);
    }

    public function testIncludeSubfoldersWithoutWildcard()
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Cannot include subfolders without wildcard.");

        $application = new Application(
            [
                "parameters" => [
                    "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                    "key" => "/notawildcard",
                    "includeSubfolders" => true,
                    "newFilesOnly" => false,
                    "limit" => 0
                ],
            ],
            [],
            new TestHandler()
        );
        $application->actionRun($this->path);
    }
}
