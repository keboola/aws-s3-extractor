<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Application;
use Monolog\Handler\TestHandler;

class ApplicationFunctionalTest extends FunctionalTestCase
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
                "limit" => 0
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
                "limit" => 0
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
