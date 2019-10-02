<?php

namespace Keboola\S3ExtractorTest\Functional;

use Aws\S3\S3Client;
use Keboola\Component\JsonHelper;
use Keboola\S3Extractor\Config;
use Keboola\S3Extractor\ConfigDefinition;
use Keboola\S3Extractor\Extractor;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class NewFilesOnlyFunctionalTest extends FunctionalTestCase
{
    public function testSuccessfulDownloadFromRoot(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-from-root',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'file1.csv',
                    'includeSubfolders' => false,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    public function testSuccessfulDownloadFromFolderUpdatedStep1(): void
    {
        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-from-updated/setp-1',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'folder2/*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    public function testSuccessfulDownloadFromFolderUpdatedStep2(): void
    {
        $updateFile = (new S3Client([
            'region' => getenv(self::UPDATE_AWS_REGION),
            'version' => '2006-03-01',
            'credentials' => [
                'key' => getenv(self::UPDATE_AWS_S3_ACCESS_KEY_ENV),
                'secret' => getenv(self::UPDATE_AWS_S3_SECRET_KEY_ENV),
            ],
        ]))->putObject([
            'Bucket' => getenv(self::UPDATE_AWS_S3_BUCKET),
            'Key' => 'folder2/file1.csv',
            'Body' => fopen(__DIR__ . '/../_S3InitData/folder2/file1.csv', 'rb+'),
        ]);

        JsonHelper::writeFile(__DIR__ . '/normal-download/expected/data/out/state.json', [
            'lastDownloadedFileTimestamp' => strtotime($updateFile->toArray()['@metadata']['headers']['date']),
            'processedFilesInLastTimestampSecond' => 'folder2/file1.csv',
        ]);

        $this->runTestWithCustomConfiguration(
            __DIR__ . '/download-from-updated/setp-2',
            [
                'parameters' => [
                    'accessKeyId' => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    '#secretAccessKey' => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    'bucket' => getenv(self::AWS_S3_BUCKET_ENV),
                    'key' => 'folder2/*',
                    'includeSubfolders' => false,
                    'newFilesOnly' => true,
                    'limit' => 0,
                ],
            ],
            0
        );
    }

    public function testSuccessfulDownloadFromFolderUpdated(): void
    {
        $key = "folder2/*";
        $testHandler = new TestHandler();
        $extractor = new Extractor(new Config([
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => $key,
                "includeSubfolders" => false,
                "newFilesOnly" => true,
                "limit" => 0,
            ],
        ], new ConfigDefinition), [], (new Logger('test'))->pushHandler($testHandler));
        $state1 = $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 3 file(s)"));
        $this->assertCount(4, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state1);
        $this->assertGreaterThan(0, $state1['lastDownloadedFileTimestamp']);

        // update file
        $client = new S3Client([
            'region' => getenv(self::UPDATE_AWS_REGION),
            'version' => '2006-03-01',
            'credentials' => [
                'key' => getenv(self::UPDATE_AWS_S3_ACCESS_KEY_ENV),
                'secret' => getenv(self::UPDATE_AWS_S3_SECRET_KEY_ENV),
            ],
        ]);
        $client->putObject([
            'Bucket' => getenv(self::UPDATE_AWS_S3_BUCKET),
            'Key' => 'folder2/file1.csv',
            'Body' => fopen(__DIR__ . '/../../../_data/folder2/file1.csv', 'r+'),
        ]);

        // download only the new file
        $testHandler = new TestHandler();
        $extractor = new Extractor(new Config([
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => $key,
                "includeSubfolders" => false,
                "newFilesOnly" => true,
                "limit" => 0,
            ],
        ], new ConfigDefinition), $state1, (new Logger('test'))->pushHandler($testHandler));
        $state2 = $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertTrue($testHandler->hasInfo("Downloading file /folder2/file1.csv"));
        $this->assertCount(2, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state2);
        $this->assertGreaterThan($state1['lastDownloadedFileTimestamp'], $state2['lastDownloadedFileTimestamp']);

        // do not download anything
        $testHandler = new TestHandler();
        $extractor = new Extractor(new Config([
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => $key,
                "includeSubfolders" => false,
                "newFilesOnly" => true,
                "limit" => 0,
            ],
        ], new ConfigDefinition), $state2, (new Logger('test'))->pushHandler($testHandler));
        $state3 = $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 0 file(s)"));
        $this->assertCount(1, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state3);
        $this->assertEquals($state3, $state2);
    }
}
