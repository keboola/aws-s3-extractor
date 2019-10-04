<?php

namespace Keboola\S3ExtractorTest\Functional;

class LimitFunctionalTest extends FunctionalTestCase
{
    public function testLimitReached(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/limit/reached',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'f*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 1,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading only 1 oldest file(s) out of 7',
                'Downloading file /file1.csv',
                'Downloaded 1 file(s)',
            ]),
            null
        );
    }

    public function testLimitNotExceeded(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/limit/not-exceeded',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'f*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 10,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /file1.csv',
                'Downloading file /folder1/file1.csv',
                'Downloading file /folder2/collision-file1.csv',
                'Downloading file /folder2/collision/file1.csv',
                'Downloading file /folder2/file1.csv',
                'Downloading file /folder2/file2.csv',
                'Downloading file /folder2/file3/file1.csv',
                'Downloaded 7 file(s)',
            ]),
            null
        );
    }

    public function testLimitNewFilesOnly(): void
    {
        $testDirectory = __DIR__ . '/limit/new-files-only';
        self::writeInStateFile($testDirectory, ['file1.csv']);
        self::writeOutStateFile($testDirectory, ['folder1/file1.csv']);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'f*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => true,
                    'limit' => 1,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading only 1 oldest file(s) out of 6',
                'Downloading file /folder1/file1.csv',
                'Downloaded 1 file(s)',
            ]),
            null
        );
    }
}
