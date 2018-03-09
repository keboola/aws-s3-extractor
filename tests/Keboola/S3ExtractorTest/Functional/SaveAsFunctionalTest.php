<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Extractor;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class SaveAsFunctionalTest extends FunctionalTestCase
{
    /**
     * @param $testFile
     * @param TestHandler $testHandler
     * @param $prefix
     * @param $saveAs
     */
    private function assertFileDownloadedFromS3($testFile, TestHandler $testHandler, $prefix, $saveAs)
    {
        $this->assertFileExists($this->path . '/' . $saveAs . '/' . $testFile);
        $this->assertFileEquals(__DIR__ . "/../../../_data" .  $prefix . '/' . $testFile, $this->path . '/' . $saveAs . '/' . $testFile);
        $this->assertTrue($testHandler->hasInfo("Downloading file {$prefix}{$testFile}"));
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
            "limit" => 1000,
            "saveAs" => "folder"
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $expectedFile = $this->path . '/folder/file1.csv';
        $this->assertFileExists($expectedFile);
        $this->assertFileEquals(__DIR__ . "/../../../_data/file1.csv", $expectedFile);
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
            "limit" => 1000,
            "saveAs" => "folder"
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $expectedFile = $this->path . '/folder/file1.csv';
        $this->assertFileExists($expectedFile);
        $this->assertFileEquals(__DIR__ . "/../../../_data/folder1/file1.csv", $expectedFile);
        $this->assertTrue($testHandler->hasInfo("Downloading file /folder1/file1.csv"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param $initialForwardSlash
     */
    public function testSuccessfulDownloadFromNestedFolder($initialForwardSlash)
    {
        $key = "folder2/f*";
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
            "limit" => 1000,
            "saveAs" => "folder"
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileDownloadedFromS3('/file1.csv', $testHandler, "/folder2", "folder");
        $this->assertFileDownloadedFromS3('/file2.csv', $testHandler, "/folder2", "folder");
        $this->assertTrue($testHandler->hasInfo("Downloaded 2 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
    }

    /**
     * @return array
     */
    public function initialForwardSlashProvider()
    {
        return [[true], [false]];
    }
}
