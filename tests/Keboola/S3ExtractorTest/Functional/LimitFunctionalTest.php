<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Extractor;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class LimitFunctionalTest extends FunctionalTestCase
{
    /**
     * @param $testFile
     * @param TestHandler $testHandler
     * @param string $prefix
     * @param string $saveAs
     */
    private function assertFileDownloadedFromS3($testFile, TestHandler $testHandler, $prefix = "", $saveAs = 'myfile.csv')
    {
        $testFileReplaced = '/' . str_replace('/', '-', str_replace('-', '--', substr($testFile, 1)));
        $this->assertFileExists($this->path . '/' . $saveAs . $testFileReplaced);
        $this->assertFileEquals(__DIR__ . "/../../../_data" . $prefix .  $testFile, $this->path . '/' . $saveAs . $testFileReplaced);
        $this->assertTrue($testHandler->hasInfo("Downloading file {$prefix}{$testFile}"));
    }

    public function testLimitReached()
    {
        $key = "f*";
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => true,
            "newFilesOnly" => false,
            "saveAs" => "myfile.csv",
            "limit" => 1
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileDownloadedFromS3('/file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloading only 1 oldest file(s) out of 7"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
    }

    public function testLimitNotExceeded()
    {
        $key = "f*";
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => true,
            "newFilesOnly" => false,
            "saveAs" => "myfile.csv",
            "limit" => 10
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileDownloadedFromS3('/file1.csv', $testHandler);
        $this->assertFileDownloadedFromS3('/folder1/file1.csv', $testHandler);
        $this->assertFileDownloadedFromS3('/folder2/file1.csv', $testHandler);
        $this->assertFileDownloadedFromS3('/folder2/file2.csv', $testHandler);
        $this->assertFileDownloadedFromS3('/folder2/file3/file1.csv', $testHandler);
        $this->assertFileDownloadedFromS3('/folder2/collision/file1.csv', $testHandler);
        $this->assertFileDownloadedFromS3('/folder2/collision-file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloaded 7 file(s)"));
        $this->assertCount(8, $testHandler->getRecords());
    }

    public function testNewFilesOnly()
    {
        $key = "f*";
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => true,
            "newFilesOnly" => true,
            "saveAs" => "myfile.csv",
            "limit" => 1
        ], [], (new Logger('test'))->pushHandler($testHandler));
        $state = $extractor->extract($this->path);

        $this->assertFileDownloadedFromS3('/file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloading only 1 oldest file(s) out of 7"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());

        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => true,
            "newFilesOnly" => true,
            "saveAs" => "myfile.csv",
            "limit" => 1
        ], $state, (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileDownloadedFromS3('/folder1/file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloading only 1 oldest file(s) out of 6"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
    }
}
