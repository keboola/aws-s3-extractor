<?php

namespace Keboola\S3ExtractorTest;

use Keboola\S3Extractor\Application;
use Monolog\Handler\TestHandler;

class ApplicationTest extends TestCase
{
    public function testApplication()
    {
        $config = [
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => "/file1.csv",
                "newFilesOnly" => false,
                "saveAs" => "myfile.csv",
                "limit" => 1000
            ]
        ];
        $testHandler = new TestHandler();
        $application = new Application($config, [], $testHandler);
        $application->actionRun($this->path);
        $this->assertTrue($testHandler->hasInfo("Downloading file /file1.csv"));
    }

    public function testApplicationStateFilenewFilesOnly()
    {
        $config = [
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => "/file1.csv",
                "newFilesOnly" => true,
                "saveAs" => "myfile.csv",
                "limit" => 1000
            ]
        ];
        $testHandler = new TestHandler();
        $application = new Application($config, [], $testHandler);
        $state = $application->actionRun($this->path);
        $this->assertTrue($testHandler->hasInfo("Downloading file /file1.csv"));
        $this->assertCount(2, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state);
        $this->assertGreaterThan(0, $state['lastDownloadedFileTimestamp']);

        $testHandler = new TestHandler();
        $application = new Application($config, $state, $testHandler);
        $newState = $application->actionRun($this->path);
        $this->assertTrue($testHandler->hasInfo("Downloaded 0 file(s)"));
        $this->assertCount(1, $testHandler->getRecords());
        $this->assertEquals($state, $newState);
    }
}
