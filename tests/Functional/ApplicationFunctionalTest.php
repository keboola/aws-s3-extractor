<?php

namespace Keboola\S3ExtractorTest\Functional;

class ApplicationFunctionalTest extends FunctionalTestCase
{
    public function testApplication(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/application/base',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => '/file1.csv',
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            null,
            null
        );
    }

    public function testApplicationStateNewFilesOnly(): void
    {
        $testDirectory = __DIR__ . '/application/state-new-files-only';
        $file = 'file1.csv';
        self::writeOutStateFile($testDirectory, [$file]);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $file,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0,
            null,
            null
        );
    }

    public function testApplicationStateFileNewFilesOnly(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/application/state-file-new-files-only',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => '/file1.csv',
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0,
            null,
            null
        );
    }
}
