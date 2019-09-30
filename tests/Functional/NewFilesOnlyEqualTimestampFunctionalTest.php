<?php

namespace Keboola\S3ExtractorTest\Functional;

class NewFilesOnlyEqualTimestampFunctionalTest extends FunctionalTestCase
{
    public function testSuccessfulDownloadFromFolderContinuouslyStep1(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-continuously/step-1',
            $this->config(),
            0
        );
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep2(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-continuously/step-2',
            $this->config(),
            0
        );
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep3(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-continuously/step-3',
            $this->config(),
            0
        );
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep4(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-continuously/step-4',
            $this->config(),
            0
        );
    }

    public function testSuccessfulDownloadFromFolderContinuouslyStep5(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-continuously/step-5',
            $this->config(),
            0
        );
    }

    /**
     * @return array
     */
    private function config(): array
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
