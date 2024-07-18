<?php

declare(strict_types=1);

namespace Keboola\S3ExtractorTest\Functional;

class DecodeContentFunctionalTest extends FunctionalTestCase
{
    public function testApplication(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/decode-content',
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
}
