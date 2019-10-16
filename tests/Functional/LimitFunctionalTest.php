<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\DatadirTests\DatadirTestSpecification;

class LimitFunctionalTest extends FunctionalTestCase
{
    public function testLimitReached(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/limit/reached',
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
            0,
            self::convertToStdout([
                'Downloading only 1 oldest file(s) out of 7',
                'Dwnloading 1 file(s) (97 B)',
                'Downloaded file /file1.csv (97 B)',
                'Downloaded 1 file(s) (97 B)',
            ]),
            null
        );
    }

    public function testLimitNotExceeded(): void
    {
        $specification = new DatadirTestSpecification(
            __DIR__ . '/limit/not-exceeded/source/data',
            0,
            null,
            null,
            __DIR__ . '/limit/not-exceeded/expected/data/out'
        );

        $tempDatadir = $this->getTempDatadir($specification);

        self::writeConfigFile($tempDatadir, [
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => 'f*',
                'includeSubfolders' => true,
                'newFilesOnly' => false,
                'limit' => 10,
            ],
        ]);

        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
        $this->assertEqualsCanonicalizing(explode(PHP_EOL, $process->getOutput()), [
            'Dwnloading 7 file(s) (827 B)',
            'Downloaded file /file1.csv (97 B)',
            'Downloaded file /folder1/file1.csv (113 B)',
            'Downloaded file /folder2/collision-file1.csv (133 B)',
            'Downloaded file /folder2/collision/file1.csv (133 B)',
            'Downloaded file /folder2/file1.csv (113 B)',
            'Downloaded file /folder2/file2.csv (113 B)',
            'Downloaded file /folder2/file3/file1.csv (125 B)',
            'Downloaded 7 file(s) (827 B)',
            '',
        ]);
    }

    public function testLimitNewFilesOnly(): void
    {
        $testDirectory = __DIR__ . '/limit/new-files-only';
        self::writeInStateFile($testDirectory, ['file1.csv']);
        self::writeOutStateFile($testDirectory, ['folder1/file1.csv']);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
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
            0,
            self::convertToStdout([
                'Downloading only 1 oldest file(s) out of 6',
                'Dwnloading 1 file(s) (113 B)',
                'Downloaded file /folder1/file1.csv (113 B)',
                'Downloaded 1 file(s) (113 B)',
            ]),
            null
        );
    }
}
