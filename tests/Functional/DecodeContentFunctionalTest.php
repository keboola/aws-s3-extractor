<?php

declare(strict_types=1);

namespace Keboola\S3ExtractorTest\Functional;

class DecodeContentFunctionalTest extends FunctionalTestCase
{
    public function testDecodingContentFalse(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/decode-content/downloaded',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => '/snappy-compressed/snappy_compressed_data.orc',
                    'newFilesOnly' => false,
                    'limit' => 0,
                    'decodeContent' => false,
                ],
            ],
            0,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 1 file(s)',
                'Downloading 1 file(s) (51 B)',
                'Downloaded file /snappy-compressed/snappy_compressed_data.orc (51 B)',
                'Downloaded 1 file(s) (51 B)',
            ]),
            null
        );
    }

    public function testDownloadFailing(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/decode-content/failed',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => '/snappy-compressed/snappy_compressed_data.orc',
                    'newFilesOnly' => false,
                    'limit' => 0,
                    'decodeContent' => true,
                ],
            ],
            2,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 1 file(s)',
                'Downloading 1 file(s) (0 B)',
                'Error executing "GetObject" on %s',
                'Error executing "GetObject" on %s',
                'Error executing "GetObject" on %s',
                'Error executing "GetObject" on %s',
            ]),
            self::convertToStdout([
                '[%s] CRITICAL: Aws\S3\Exception\S3Exception:Error executing "GetObject" on %s',
            ])
        );
    }
}
