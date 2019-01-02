<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Extractor;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class SubfolderMatchingWildcardKeyFunctionalTest extends FunctionalTestCase
{
    public function testSuccessfulDownloadFromRoot()
    {
        $key = "collision*";
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => false,
            "limit" => 0
        ], [], (new Logger("test"))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileExists($this->path . "/collision-file1.csv");
        $this->assertFileEquals(__DIR__ . "/../../../_data/collision-file1.csv", $this->path . "/collision-file1.csv");
        $this->assertTrue($testHandler->hasInfo("Downloading file /collision-file1.csv"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
    }

    public function testSuccessfulDownloadFromSubfolder()
    {
        $key = "folder2/collision*";
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key,
            "includeSubfolders" => false,
            "newFilesOnly" => false,
            "limit" => 0
        ], [], (new Logger("test"))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileExists($this->path . "/collision-file1.csv");
        $this->assertFileEquals(__DIR__ . "/../../../_data/folder2/collision-file1.csv", $this->path . "/collision-file1.csv");
        $this->assertTrue($testHandler->hasInfo("Downloading file /folder2/collision-file1.csv"));
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
    }
}
