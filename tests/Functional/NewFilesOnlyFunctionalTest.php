<?php

namespace Keboola\S3ExtractorTest\Functional;

class NewFilesOnlyFunctionalTest extends FunctionalTestCase
{
    use RunTestByStep;
    private const TEST_UPDATED_DIRECTORY = 'download-from-updated';

    public function testSuccessfulDownloadFromRoot(): void
    {
        $testDirectory = __DIR__ . '/download-from-root';
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

    public function testSuccessfulDownloadFromFolderUpdatedStep1(): void
    {
        $this->runTestByStep(1, [
            'folder2/file2.csv',
        ]);
    }

    public function testSuccessfulDownloadFromFolderUpdatedStep2(): void
    {
        self::s3Client()->putObject([
            'Bucket' => getenv(self::UPDATE_AWS_S3_BUCKET),
            'Key' => 'folder2/file1.csv',
            'Body' => fopen(__DIR__ . '/../_S3InitData/folder2/file1.csv', 'rb+'),
        ]);

        $this->runTestByStep(2, ['folder2/file1.csv'], ['folder2/file2.csv']);
    }

    public function testSuccessfulDownloadFromFolderUpdatedStep3(): void
    {
        $this->runTestByStep(3, [
            'folder2/file2.csv',
            'folder2/file1.csv',
        ], [
            'folder2/file2.csv',
            'folder2/file1.csv',
        ]);
    }

    /**
     * @return string
     */
    protected static function baseTestDirectory(): string
    {
        return self::TEST_UPDATED_DIRECTORY;
    }

    /**
     * @return array
     */
    protected static function baseConfig(): array
    {
        return [
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => 'folder2/*',
                'includeSubfolders' => false,
                'newFilesOnly' => true,
                'limit' => 0,
            ],
        ];
    }
}
