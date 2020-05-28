<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\ConfigDefinition;

class ApplicationFunctionalTest extends FunctionalTestCase
{
    public function testApplication(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/application/base',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => '/file1.csv',
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 1 file(s)',
                'Downloading 1 file(s) (97 B)',
                'Downloaded file /file1.csv (97 B)',
                'Downloaded 1 file(s) (97 B)',
            ]),
            null
        );
    }


    public function testApplicationWithLoginViaRole(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/application/base',
            [
                'parameters' => [
                    'loginType' => ConfigDefinition::LOGIN_TYPE_ROLE,
                    'accountId' => getenv(self::ACCOUNT_ID),
                    'roleName' => getenv(self::ROLE_NAME),
                    'externalId' => getenv(self::EXTERNAL_ID),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => '/file1.csv',
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 1 file(s)',
                'Downloading 1 file(s) (97 B)',
                'Downloaded file /file1.csv (97 B)',
                'Downloaded 1 file(s) (97 B)',
            ]),
            null
        );
    }

    public function testApplicationStateNewFilesOnly(): void
    {
        $testDirectory = __DIR__ . '/application/state-new-files-only';
        $file = 'file1.csv';
        self::writeOutStateFile($testDirectory, [$file]);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $file,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 1 file(s)',
                'There are 1 new file(s)',
                'Downloading 1 file(s) (97 B)',
                'Downloaded file /file1.csv (97 B)',
                'Downloaded 1 file(s) (97 B)',
            ]),
            null
        );
    }

    public function testApplicationStateFileNewFilesOnly(): void
    {
        $testDirectory = __DIR__ . '/application/state-file-new-files-only';
        $file = 'file1.csv';
        self::writeInStateFile($testDirectory, [$file]);
        self::writeOutStateFile($testDirectory, [$file]);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $file,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Listing files to be downloaded',
                'Found 1 file(s)',
                'There are 0 new file(s)',
                'Downloaded 0 file(s) (0 B)',
            ]),
            null
        );
    }
}
