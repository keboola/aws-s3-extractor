<?php

namespace Keboola\S3ExtractorTest;

use Keboola\S3Extractor\Extractor;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class OneFileTest extends TestCase
{
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
