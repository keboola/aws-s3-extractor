<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\Component\JsonHelper;

trait RunTestByStep
{
    /**
     * @param int $step
     * @param array $processedFilesOut
     * @param array|null $processedFilesIn
     * @throws JsonHelper\JsonHelperException
     */
    protected function runTestByStep(int $step, array $processedFilesOut, array $processedFilesIn = null): void
    {
        $testDirectory = sprintf('%s/%s/step-%s', __DIR__, self::baseTestDirectory(), $step);

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

        $this->runTestWithCustomConfiguration($testDirectory, self::baseConfig(), 0);
    }

    /**
     * @return string
     */
    abstract protected static function baseTestDirectory(): string;

    /**
     * @return array
     */
    abstract protected static function baseConfig(): array;
}