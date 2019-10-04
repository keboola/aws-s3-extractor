<?php

namespace Keboola\S3ExtractorTest\Functional;

class SaveAsFunctionalTest extends FunctionalTestCase
{
    /**
     * @dataProvider initialForwardSlashProvider
     * @param bool $initialForwardSlash
     */
    public function testSuccessfulDownloadFromRoot(bool $initialForwardSlash): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/save-as/download-from-root',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/file1.csv' : 'file1.csv',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                    'saveAs' => 'folder',
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /file1.csv',
                'Downloaded 1 file(s)',
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
            __DIR__ . '/save-as/download-from-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/folder1/file1.csv' : 'folder1/file1.csv',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                    'saveAs' => 'folder',
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /folder1/file1.csv',
                'Downloaded 1 file(s)',
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
            __DIR__ . '/save-as/download-from-nested-folder',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => $initialForwardSlash ? '/folder2/f*' : 'folder2/f*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => false,
                    'limit' => 0,
                    'saveAs' => 'folder',
                ],
            ],
            0,
            self::convertToStdout([
                'Downloading file /folder2/file1.csv',
                'Downloading file /folder2/file2.csv',
                'Downloaded 2 file(s)',
            ]),
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
