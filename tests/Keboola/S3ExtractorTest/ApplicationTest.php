<?php
namespace Keboola\S3ExtractorTest;

use Keboola\S3Extractor\Application;
use Keboola\S3Extractor\Exception;
use Monolog\Handler\NullHandler;
use Monolog\Handler\TestHandler;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class ApplicationTest extends TestCase
{
    const AWS_REGION_ENV = 'AWS_REGION';
    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';
    protected $path = '/tmp/application';

    public function setUp()
    {
        if (!file_exists($this->path)) {
            mkdir($this->path);
        }
    }

    public function tearDown()
    {
        passthru('rm -rf ' . $this->path);
    }

    public function testApplication()
    {
        $config = [
            "parameters" => [
                "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                "key" => "/file1.csv",
                "newFilesOnly" => false
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
                "newFilesOnly" => true
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
