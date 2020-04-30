<?php

namespace Keboola\S3ExtractorTest\Functional;

class NewFilesOnlyFunctionalTest extends FunctionalTestCase
{
    public function testSuccessfulDownloadFromRoot(): void
    {
        $testDirectory = __DIR__ . '/new-files-only/download-from-root';
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
                    'includeSubfolders' => false,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 1 file(s)',
                'There are 1 new file(s)',
                'Downloading 1 file(s) (97 B)',
                'Downloaded file /file1.csv (97 B)',
                'Downloaded 1 file(s) (97 B)',
            ]),
            null
        );
    }

    public function testSuccessfulDownloadFromFolderUpdated(): void
    {
        $lastModified = self::getS3FileLastModified('folder2/file1.csv');
        self::s3Client()->putObject([
            'Bucket' => getenv(self::UPDATE_AWS_S3_BUCKET),
            'Key' => 'folder2/file1.csv',
            'Body' => fopen(__DIR__ . '/../_S3InitData/folder2/file1.csv', 'rb+'),
        ]);

        $testDirectory = __DIR__ . '/new-files-only/download-from-updated';
        self::writeInStateFile($testDirectory, ['folder2/file2.csv'], $lastModified);
        self::writeOutStateFile($testDirectory, ['folder2/file1.csv']);

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
            0,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 3 file(s)',
                'There are 1 new file(s)',
                'Downloading 1 file(s) (113 B)',
                'Downloaded file /folder2/file1.csv (113 B)',
                'Downloaded 1 file(s) (113 B)',
            ]),
            null
        );
    }
}
