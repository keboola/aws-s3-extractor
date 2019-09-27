<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Application;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class ApplicationFunctionalTest extends FunctionalTestCase
{
    public function testApplication()
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->path));

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

        $this->writeConfig($config);

        $handler = new TestHandler;
        (new Application(
            (new Logger('s3Test'))->pushHandler($handler)
        ))->execute();

        $this->assertTrue($handler->hasInfo("Downloading file /file1.csv"));
    }

    public function testApplicationStateFileNewFilesOnly()
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->path));

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

        $this->writeConfig($config);

        $handler = new TestHandler;
        (new Application((new Logger('s3Test1'))->pushHandler($handler)))->execute();
        $this->assertTrue($handler->hasInfo("Downloading file /file1.csv"));
        $this->assertCount(3, $handler->getRecords());
        $state = $this->getOutputState();
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state);
        $this->assertGreaterThan(0, $state['lastDownloadedFileTimestamp']);

        $this->syncInputState();

        $handler = new TestHandler;
        (new Application((new Logger('s3Test1'))->pushHandler($handler)))->execute();
        $this->assertTrue($handler->hasInfo("Downloaded 0 file(s)"));
        $this->assertCount(2, $handler->getRecords());
        $this->assertEquals($state, $this->getOutputState());
    }
}
