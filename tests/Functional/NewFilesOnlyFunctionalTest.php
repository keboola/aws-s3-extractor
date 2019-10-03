<?php

namespace Keboola\S3ExtractorTest\Functional;

class NewFilesOnlyFunctionalTest extends FunctionalTestCase
{
    public function testSuccessfulDownloadFromRoot(): void
    {
        $testDirectory = __DIR__ . '/new-files-only/download-from-root';
        $file = 'file1.csv';
        self::writeStateOut($testDirectory, [$file]);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $file,
                    'includeSubfolders' => false,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    public function testSuccessfulDownloadFromFolderUpdated(): void
    {
        $lastModified = self::s3FileLastModified('folder2/file1.csv');
        self::s3Client()->putObject([
            'Bucket' => getenv(self::UPDATE_AWS_S3_BUCKET),
            'Key' => 'folder2/file1.csv',
            'Body' => fopen(__DIR__ . '/../_S3InitData/folder2/file1.csv', 'rb+'),
        ]);

        $testDirectory = __DIR__ . '/new-files-only/download-from-updated';
        self::writeStateIn($testDirectory, ['folder2/file2.csv'], $lastModified);
        self::writeStateOut($testDirectory, ['folder2/file1.csv']);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'folder2/*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0
        );
    }
}
