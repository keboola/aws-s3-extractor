<?php
namespace Keboola\S3ExtractorTest;

use Keboola\S3Extractor\Extractor;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class OneFileTest extends TestCase
{
    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';

    protected $path;

    public function setUp()
    {
        $this->path = '/tmp/aws-s3-extractor/' . uniqid();
        mkdir($this->path, 0777, true);
    }

    public function tearDown()
    {
        passthru('rm -rf ' . $this->path);
    }

    /**
     * @dataProvider initialForwardSlashProvider
     */
    public function testSuccessfulDownloadFromRoot($initialForwardSlash)
    {
        $key = "file1.csv";
        if ($initialForwardSlash) {
            $key = "/" . $key;
        }
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => false,
            "saveAs" => "myfile.csv"
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $expectedFile = $this->path . '/' . 'myfile.csv';
        $this->assertFileExists($expectedFile);
        $this->assertFileEquals(__DIR__ . "/../../_data/file1.csv", $expectedFile);
        $this->assertTrue($testHandler->hasInfo("Downloading file /file1.csv"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
    }

    /**
     * @dataProvider initialForwardSlashProvider
     */
    public function testSuccessfulDownloadFromFolder($initialForwardSlash)
    {
        $key = "folder1/file1.csv";
        if ($initialForwardSlash) {
            $key = "/" . $key;
        }
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => false,
            "saveAs" => "myfile.csv"
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $expectedFile = $this->path . '/' . 'myfile.csv';
        $this->assertFileExists($expectedFile);
        $this->assertFileEquals(__DIR__ . "/../../_data/folder1/file1.csv", $expectedFile);
        $this->assertTrue($testHandler->hasInfo("Downloading file /folder1/file1.csv"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
    }

    /**
     * @return array
     */
    public function initialForwardSlashProvider()
    {
        return [[true], [false]];
    }
}
