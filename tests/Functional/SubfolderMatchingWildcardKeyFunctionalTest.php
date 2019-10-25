<?php

namespace Keboola\S3ExtractorTest\Functional;

class SubfolderMatchingWildcardKeyFunctionalTest extends FunctionalTestCase
{
    public function testSuccessfulDownloadFromRoot(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolder-matching-wildcard-key/download-from-root',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'collision*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Dwnloading 1 file(s) (117 B)',
                'Downloaded file /collision-file1.csv (117 B)',
                'Downloaded 1 file(s) (117 B)',
            ]),
            null
        );
    }

    public function testSuccessfulDownloadFromSubfolder(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolder-matching-wildcard-key/download-from-subfolder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'folder2/collision*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Dwnloading 1 file(s) (133 B)',
                'Downloaded file /folder2/collision-file1.csv (133 B)',
                'Downloaded 1 file(s) (133 B)',
            ]),
            null
        );
    }
}
