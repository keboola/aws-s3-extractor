<?php

namespace Keboola\S3ExtractorTest\Functional;

class WildcardKeyFunctionalTest extends FunctionalTestCase
{
    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromRoot($initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/wildcard-key/download-from-root',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/f*' : 'f*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulCollisionDownloadFromRoot($initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/wildcard-key/collision-download-from-root',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/c*' : 'c*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromFolder(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/wildcard-key/download-from-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/folder2/*' : 'folder2/*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromNestedFolder(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/wildcard-key/download-from-nested-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/folder2/file3/*' : 'folder2/file3/*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadEmptyFolderWithoutTrailingForwardslash($initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/wildcard-key/download-empty-folder-without-slash',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/emptyfolder*' : 'emptyfolder*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromEmptyFolder(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/wildcard-key/download-from-empty-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/emptyfolder/*' : 'emptyfolder/*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testNoFilesDownloaded(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/wildcard-key/no-files-downloaded',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/nonexiting*' : 'nonexiting*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0
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
