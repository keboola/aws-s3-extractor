<?php

namespace Keboola\S3ExtractorTest\Functional;

class NewFilesOnlyEqualTimestampFunctionalTest extends FunctionalTestCase
{
    use RunTestByStep;
    private const TEST_DIRECTORY = 'download-continuously';

    public function testSuccessfulDownloadFromFolderContinuouslyStep1(): void
    {
        $this->runTestByStep(1, [
            'no-unique-timestamps/folder2/collision-file1.csv',
        ]);
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep2(): void
    {
        $this->runTestByStep(2, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
        ], [
            'no-unique-timestamps/folder2/collision-file1.csv',
        ]);
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep3(): void
    {
        $this->runTestByStep(3, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
        ], [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
        ]);
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep4(): void
    {
        $this->runTestByStep(4, [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
            'no-unique-timestamps/folder2/file2.csv',
        ], [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
        ]);
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep5(): void
    {
        $this->runTestByStep(5, [
            'no-unique-timestamps/folder2/file3/file1.csv',
        ], [
            'no-unique-timestamps/folder2/collision-file1.csv',
            'no-unique-timestamps/folder2/collision/file1.csv',
            'no-unique-timestamps/folder2/file1.csv',
            'no-unique-timestamps/folder2/file2.csv',
        ]);
    }

    /**
     * @return string
     */
    protected static function baseTestDirectory(): string
    {
        return self::TEST_DIRECTORY;
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
                'key' => 'no-unique-timestamps/*',
                'includeSubfolders' => true,
                'newFilesOnly' => true,
                'limit' => 1,
            ],
        ];
    }
}
