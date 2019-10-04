<?php

namespace Keboola\S3ExtractorTest\Functional;

class NewFilesOnlyEqualTimestampFunctionalTest extends FunctionalTestCase
{
    public function testSuccessfulDownloadFromFolderContinuously1(): void
    {
        $testDirectory = __DIR__ . '/new-files-only-equal-timestamp/download-continuously-1';
        self::writeOutStateFile($testDirectory, ['no-unique-timestamps/folder2/collision-file1.csv']);
        $this->runTestWithCustomConfiguration($testDirectory, self::config(), 0, null, null);
    }

    public function testSuccessfulDownloadFromFolderContinuously2(): void
    {
        $testDirectory = __DIR__ . '/new-files-only-equal-timestamp/download-continuously-2';
        self::writeInStateFile($testDirectory, ['no-unique-timestamps/folder2/collision-file1.csv']);
        self::writeOutStateFile($testDirectory, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
        ]);
        $this->runTestWithCustomConfiguration($testDirectory, self::config(), 0, null, null);
    }

    public function testSuccessfulDownloadFromFolderContinuously3(): void
    {
        $testDirectory = __DIR__ . '/new-files-only-equal-timestamp/download-continuously-3';
        self::writeInStateFile($testDirectory, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
        ]);
        self::writeOutStateFile($testDirectory, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
        ]);
        $this->runTestWithCustomConfiguration($testDirectory, self::config(), 0, null, null);
    }

    public function testSuccessfulDownloadFromFolderContinuously4(): void
    {
        $testDirectory = __DIR__ . '/new-files-only-equal-timestamp/download-continuously-4';
        self::writeInStateFile($testDirectory, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
        ]);
        self::writeOutStateFile($testDirectory, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
            'no-unique-timestamps/folder2/file2.csv',
        ]);
        $this->runTestWithCustomConfiguration($testDirectory, self::config(), 0, null, null);
    }

    public function testSuccessfulDownloadFromFolderContinuousl5(): void
    {
        $testDirectory = __DIR__ . '/new-files-only-equal-timestamp/download-continuously-5';
        self::writeInStateFile($testDirectory, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
            'no-unique-timestamps/folder2/file2.csv',
        ]);
        self::writeOutStateFile($testDirectory, ['no-unique-timestamps/folder2/file3/file1.csv']);
        $this->runTestWithCustomConfiguration($testDirectory, self::config(), 0, null, null);
    }

    public function testSuccessfulDownloadFromFolderContinuousl6(): void
    {
        $testDirectory = __DIR__ . '/new-files-only-equal-timestamp/download-continuously-6';
        self::writeInStateFile($testDirectory, ['no-unique-timestamps/folder2/file3/file1.csv']);
        self::writeOutStateFile($testDirectory, ['no-unique-timestamps/folder2/file3/file1.csv']);
        $this->runTestWithCustomConfiguration($testDirectory, self::config(), 0, null, null);
    }

    /**
     * @return array
     */
    private static function config(): array
    {
        return [
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => 'no-unique-timestamps/*',
                'includeSubfolders' => true,
                'newFilesOnly' => true,
                'limit' => 1,
            ],
        ];
    }
}
