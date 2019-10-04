<?php

namespace Keboola\S3ExtractorTest\Functional;

class SubfoldersFunctionalTest extends FunctionalTestCase
{
    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromRoot(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolders/download-from-root',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/f*' : 'f*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /file1.csv',
                'Downloading file /folder1/file1.csv',
                'Downloading file /folder2/collision-file1.csv',
                'Downloading file /folder2/collision/file1.csv',
                'Downloading file /folder2/file1.csv',
                'Downloading file /folder2/file2.csv',
                'Downloading file /folder2/file3/file1.csv',
                'Downloaded 7 file(s)',
            ]),
            null
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulCollisionDownloadFromRoot(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolders/collision-download-from-root',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/c*' : 'c*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /collision-file1.csv',
                'Downloading file /collision/file1.csv',
                'Downloaded 2 file(s)',
            ]),
            null
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromFolder(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolders/download-from-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/folder2/*' : 'folder2/*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /folder2/collision-file1.csv',
                'Downloading file /folder2/collision/file1.csv',
                'Downloading file /folder2/file1.csv',
                'Downloading file /folder2/file2.csv',
                'Downloading file /folder2/file3/file1.csv',
                'Downloaded 5 file(s)',
            ]),
            null
        );
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromAmbiguousPrefix(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/subfolders/download-from-ambiguous-prefix',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/folder2/collision*' : 'folder2/collision*',
                    'includeSubfolders' => true,
                    'newFilesOnly' => false,
                    'limit' => 0,
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /folder2/collision-file1.csv',
                'Downloading file /folder2/collision/file1.csv',
                'Downloaded 2 file(s)',
            ]),
            null
        );
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
                'Downloading file /folder2/file3/file1.csv',
                'Downloaded 1 file(s)',
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
            self::convertToStdout(['Downloaded 0 file(s)']),
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
            self::convertToStdout(['Downloaded 0 file(s)']),
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
