<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Application;
use Keboola\Component\UserException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class ExceptionsFunctionalTest extends FunctionalTestCase
{
    public function testInvalidBucket()
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->path));

        $this->writeConfig([
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV) . "_invalid",
                "key" => "/file1.csv",
                "includeSubfolders" => false,
                "newFilesOnly" => false,
                "limit" => 0
            ],
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage("404 Not Found (NoSuchBucket)");
        $this->expectExceptionMessage("The specified bucket does not exist");
        $this->expectExceptionMessage(getenv(self::AWS_S3_BUCKET_ENV) . "_invalid");

        (new Application(
            (new Logger('s3Test'))->pushHandler(new TestHandler)
        ))->execute();
    }

    public function testInvalidCredentials()
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->path));

        $this->writeConfig([
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV) . "_invalid",
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => "/file1.csv",
                "includeSubfolders" => false,
                "newFilesOnly" => false,
                "limit" => 0
            ],
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage("Invalid credentials or permissions.");

        (new Application(
            (new Logger('s3Test'))->pushHandler(new TestHandler)
        ))->execute();
    }

    public function testInvalidKey()
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->path));

        $this->writeConfig([
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => "/doesnotexist",
                "includeSubfolders" => false,
                "newFilesOnly" => false,
                "limit" => 0
            ],
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Error 404: Key "/doesnotexist" not found.');

        (new Application(
            (new Logger('s3Test'))->pushHandler(new TestHandler)
        ))->execute();
    }

    /**
     * @dataProvider incorrectKeyProvider
     */
    public function testMissingWildcardOrPathFile(string $key): void
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->path));

        $this->writeConfig([
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => $key,
            ],
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Use the wildcard flag or enter a full path to the file.');

        (new Application(
            (new Logger('s3Test'))->pushHandler(new TestHandler)
        ))->execute();
    }

    /**
     * @return array
     */
    public function incorrectKeyProvider(): array
    {
        return [
            ['foo/bar/'],
            ['FooBar/'],
            ['*/'],
            ['//'],
            ['/'],
        ];
    }

    public function testIncludeSubfoldersWithoutWildcard()
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->path));

        $this->writeConfig([
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => "/notawildcard",
                "includeSubfolders" => true,
                "newFilesOnly" => false,
                "limit" => 0
            ],
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage("Cannot include subfolders without wildcard.");

        (new Application(
            (new Logger('s3Test'))->pushHandler(new TestHandler)
        ))->execute();
    }
}
