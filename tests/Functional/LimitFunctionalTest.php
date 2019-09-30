<?php

namespace Keboola\S3ExtractorTest\Functional;

class LimitFunctionalTest extends FunctionalTestCase
{
    public function testLimitReached()
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/limit-reached',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'f*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 1,
                ],
            ],
            0
        );
    }

    public function testLimitNotExceeded()
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/limit-not-exceeded',
            [
                'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => 'f*',
                'includeSubfolders' => true,
                'newFilesOnly' => false,
                'limit' => 10,
                ],
            ],
            0
        );
    }

    public function testLimitNewFilesOnly()
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/limit-new-files-only',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'f*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => true,
                    'limit' => 1,
                ],
            ],
            0
        );
    }
}
