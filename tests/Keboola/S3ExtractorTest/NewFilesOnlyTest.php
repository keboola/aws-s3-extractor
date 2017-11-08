<?php
namespace Keboola\S3ExtractorTest;

use Keboola\S3Extractor\Extractor;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class NewFilesOnlyTest extends TestCase
{
    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';
    const UPDATE_AWS_S3_ACCESS_KEY_ENV = 'UPLOAD_USER_AWS_ACCESS_KEY';
    const UPDATE_AWS_S3_SECRET_KEY_ENV = 'UPLOAD_USER_AWS_SECRET_KEY';
    const UPDATE_AWS_S3_BUCKET = 'AWS_S3_BUCKET';
    const UPDATE_AWS_REGION = 'AWS_REGION';

    protected $path;

    public function setUp()
    {
        $this->path = '/tmp/aws-s3-extractor/' . uniqid();
        mkdir($this->path, 0777, true);
    }

    public function tearDown()
    {
        system('rm -rf ' . $this->path);
    }

    public function testSuccessfulDownloadFromRoot()
    {
        $key = "file1.csv";
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => true,
            "saveAs" => "myfile.csv"
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $state = $extractor->extract($this->path);

        $expectedFile = $this->path . '/' . 'myfile.csv';
        $this->assertFileExists($expectedFile);
        $this->assertFileEquals(__DIR__ . "/../../_data/file1.csv", $expectedFile);
        $this->assertTrue($testHandler->hasInfo("Downloading file /file1.csv"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state);
        $this->assertGreaterThan(0, $state['lastDownloadedFileTimestamp']);
    }

    public function testSuccessfulDownloadFromFolderUpdated()
    {
        $key = "folder2/*";
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => true,
            "saveAs" => "myfile.csv"
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $state1 = $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 3 file(s)"));
        $this->assertCount(4, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state1);
        $this->assertGreaterThan(0, $state1['lastDownloadedFileTimestamp']);

        // update file
        $client =  new \Aws\S3\S3Client([
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
            'Body' => fopen(__DIR__ . '/../../_data/folder2/file1.csv', 'r+')
        ]);

        // download only the new file
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => true,
            "saveAs" => "myfile.csv"
        ], $state1, (new Logger('test'))->pushHandler($testHandler));
        $state2 = $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertTrue($testHandler->hasInfo("Downloading file /folder2/file1.csv"));
        $this->assertCount(2, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state2);
        $this->assertGreaterThan($state1['lastDownloadedFileTimestamp'], $state2['lastDownloadedFileTimestamp']);

        // do not dowlnoad anything
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => true,
            "saveAs" => "myfile.csv"
        ], $state2, (new Logger('test'))->pushHandler($testHandler));
        $state3 = $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 0 file(s)"));
        $this->assertCount(1, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state3);
        $this->assertEquals($state3, $state2);
    }
}
