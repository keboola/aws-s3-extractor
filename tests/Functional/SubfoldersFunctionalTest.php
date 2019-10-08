<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\DatadirTests\DatadirTestSpecification;

class SubfoldersFunctionalTest extends FunctionalTestCase
{
    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromRoot(bool $initialForwardSlash): void
    {
        $specification = new DatadirTestSpecification(
            __DIR__ . '/subfolders/download-from-root/source/data',
            0,
            null,
            null,
            __DIR__ . '/subfolders/download-from-root/expected/data/out'
        );

        $tempDatadir = $this->getTempDatadir($specification);

        self::writeConfigFile($tempDatadir, [
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => $initialForwardSlash ? '/f*' : 'f*',
                'includeSubfolders' => true,
                'newFilesOnly' => false,
                'limit' => 0,
            ],
        ]);

        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
        $this->assertEqualsCanonicalizing(explode(PHP_EOL, $process->getOutput()), [
            'Downloading file complete /file1.csv (97 B)',
            'Downloading file complete /folder1/file1.csv (113 B)',
            'Downloading file complete /folder2/collision-file1.csv (133 B)',
            'Downloading file complete /folder2/collision/file1.csv (133 B)',
            'Downloading file complete /folder2/file1.csv (113 B)',
            'Downloading file complete /folder2/file2.csv (113 B)',
            'Downloading file complete /folder2/file3/file1.csv (125 B)',
            'Downloaded 7 file(s) (827 B)',
            '',
        ]);
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulCollisionDownloadFromRoot(bool $initialForwardSlash): void
    {
        $specification = new DatadirTestSpecification(
            __DIR__ . '/subfolders/collision-download-from-root/source/data',
            0,
            null,
            null,
            __DIR__ . '/subfolders/collision-download-from-root/expected/data/out'
        );

        $tempDatadir = $this->getTempDatadir($specification);

        self::writeConfigFile($tempDatadir, [
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => $initialForwardSlash ? '/c*' : 'c*',
                'includeSubfolders' => true,
                'newFilesOnly' => false,
                'limit' => 0,
            ],
        ]);

        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
        $this->assertEqualsCanonicalizing(explode(PHP_EOL, $process->getOutput()), [
            'Downloading file complete /collision-file1.csv (117 B)',
            'Downloading file complete /collision/file1.csv (117 B)',
            'Downloaded 2 file(s) (234 B)',
            '',
        ]);
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromFolder(bool $initialForwardSlash): void
    {
        $specification = new DatadirTestSpecification(
            __DIR__ . '/subfolders/download-from-folder/source/data',
            0,
            null,
            null,
            __DIR__ . '/subfolders/download-from-folder/expected/data/out'
        );

        $tempDatadir = $this->getTempDatadir($specification);

        self::writeConfigFile($tempDatadir, [
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => $initialForwardSlash ? '/folder2/*' : 'folder2/*',
                'includeSubfolders' => true,
                'newFilesOnly' => false,
                'limit' => 0,
            ],
        ]);

        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
        $this->assertEqualsCanonicalizing(explode(PHP_EOL, $process->getOutput()), [
            'Downloading file complete /folder2/collision-file1.csv (133 B)',
            'Downloading file complete /folder2/collision/file1.csv (133 B)',
            'Downloading file complete /folder2/file1.csv (113 B)',
            'Downloading file complete /folder2/file2.csv (113 B)',
            'Downloading file complete /folder2/file3/file1.csv (125 B)',
            'Downloaded 5 file(s) (617 B)',
            '',
        ]);
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromAmbiguousPrefix(bool $initialForwardSlash): void
    {
        $specification = new DatadirTestSpecification(
            __DIR__ . '/subfolders/download-from-ambiguous-prefix/source/data',
            0,
            null,
            null,
            __DIR__ . '/subfolders/download-from-ambiguous-prefix/expected/data/out'
        );

        $tempDatadir = $this->getTempDatadir($specification);

        self::writeConfigFile($tempDatadir, [
            'parameters' => [
                'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                'key' => $initialForwardSlash ? '/folder2/collision*' : 'folder2/collision*',
                'includeSubfolders' => true,
                'newFilesOnly' => false,
                'limit' => 0,
            ],
        ]);

        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
        $this->assertEqualsCanonicalizing(explode(PHP_EOL, $process->getOutput()), [
            'Downloading file complete /folder2/collision-file1.csv (133 B)',
            'Downloading file complete /folder2/collision/file1.csv (133 B)',
            'Downloaded 2 file(s) (266 B)',
            '',
        ]);
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromNestedFolder(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolders/download-from-nested-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/folder2/file3/*' : 'folder2/file3/*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file complete /folder2/file3/file1.csv (125 B)',
                'Downloaded 1 file(s) (125 B)',
            ]),
            null
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromEmptyFolder(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolders/download-from-empty-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/emptyfolder/*' : 'emptyfolder/*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout(['Downloaded 0 file(s) (0 B)']),
            null
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testNoFilesDownloaded(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolders/no-files-downloaded',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/nonexiting*' : 'nonexiting*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout(['Downloaded 0 file(s) (0 B)']),
            null
        );
    }

    /**
     * @return array
     */
    public function initialForwardSlashProvider(): array
    {
        return [[true], [false]];
    }
}
