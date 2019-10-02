<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\Component\JsonHelper;

class NewFilesOnlyEqualTimestampFunctionalTest extends FunctionalTestCase
{
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
     * @param int $step
     * @param array $processedFilesOut
     * @param array|null $processedFilesIn
     * @throws JsonHelper\JsonHelperException
     */
    private function runTestByStep(int $step, array $processedFilesOut, array $processedFilesIn = null): void
    {
        $testDirectory = sprintf('%s/%s/step-%s', __DIR__, self::TEST_DIRECTORY, $step);

        JsonHelper::writeFile(sprintf('/%s/expected/data/out/state.json', $testDirectory), [
            'lastDownloadedFileTimestamp' => self::s3FileLastModified($processedFilesOut[0]),
            'processedFilesInLastTimestampSecond' => $processedFilesOut,
        ]);

        if ($processedFilesIn) {
            JsonHelper::writeFile(sprintf('/%s/source/data/in/state.json', $testDirectory), [
                'lastDownloadedFileTimestamp' => self::s3FileLastModified($processedFilesIn[0]),
                'processedFilesInLastTimestampSecond' => $processedFilesIn,
            ]);
        }

        $this->runTestWithCustomConfiguration($testDirectory, self::config(), 0);
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
