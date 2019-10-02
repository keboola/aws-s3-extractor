<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\Component\JsonHelper;

class ApplicationFunctionalTest extends FunctionalTestCase
{
    public function testApplication(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/application-run',
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
            0
        );
    }

    public function testApplicationStateNewFilesOnly(): void
    {
        $key = 'file1.csv';
        JsonHelper::writeFile(__DIR__ . '/application-state-new-files-only/expected/data/out/state.json', [
            'lastDownloadedFileTimestamp' =>  self::s3FileLastModified($key),
            'processedFilesInLastTimestampSecond' => [$key],
        ]);

        $this->runTestWithCustomConfiguration(
            __DIR__ . '/application-state-new-files-only',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $key,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    public function testApplicationStateFileNewFilesOnly(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/application-state-file-new-files-only',
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
            0
        );
    }
}
