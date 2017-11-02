<?php
namespace Keboola\S3ExtractorTest;

use Keboola\S3Extractor\Extractor;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class WildcardKeyTest extends TestCase
{
    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';

    protected $path = '/tmp/wildcard';

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

    /**
     * @param $testFile
     * @param TestHandler $testHandler
     * @param string $prefix
     */
    private function assertFileDownloadedFromS3($testFile, TestHandler $testHandler, $prefix = "")
    {
        $this->assertFileExists($this->path . $testFile);
        $this->assertFileEquals(__DIR__ . "/../../_data" . $prefix .  $testFile, $this->path . $testFile);
        $this->assertTrue($testHandler->hasInfo("Downloading file {$prefix}{$testFile}"));
    }

    /**
     * @param $testFile
     */
    private function assertFileNotDownloadedFromS3($testFile)
    {
        $this->assertFileNotExists($this->path . $testFile);
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param $initialForwardSlash
     */
    public function testSuccessfulDownloadFromRoot($initialForwardSlash)
    {
        $key = "f*";
        if ($initialForwardSlash) {
            $key = "/" . $key;
        }
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key
        ], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileDownloadedFromS3('/file1.csv', $testHandler);
        $this->assertFileNotDownloadedFromS3('/folder1/file1.csv');
        $this->assertFileNotDownloadedFromS3('/folder2/file1.csv');
        $this->assertFileNotDownloadedFromS3('/folder2/file2.csv');
        $this->assertFileNotDownloadedFromS3('/folder2/file3/file1.csv');
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param $initialForwardSlash
     */
    public function testSuccessfulDownloadFromFolder($initialForwardSlash)
    {
        $key = "folder2/*";
        if ($initialForwardSlash) {
            $key = "/" . $key;
        }
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key
        ], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertFileDownloadedFromS3('/file1.csv', $testHandler, "/folder2");
        $this->assertFileDownloadedFromS3('/file2.csv', $testHandler, "/folder2");
        $this->assertFileNotDownloadedFromS3('/file3/file1.csv');

        $this->assertTrue($testHandler->hasInfo("Downloaded 2 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param $initialForwardSlash
     */
    public function testSuccessfulDownloadFromEmptyFolder($initialForwardSlash)
    {
        $key = "emptyfolder/*";
        if ($initialForwardSlash) {
            $key = "/" . $key;
        }
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key
        ], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 0 file(s)"));
        $this->assertCount(1, $testHandler->getRecords());
    }

    /**
     * @dataProvider initialForwardSlashProvider
     * @param $initialForwardSlash
     */
    public function testNoFilesDownloaded($initialForwardSlash)
    {
        $key = "nonexiting*";
        if ($initialForwardSlash) {
            $key = "/" . $key;
        }
        $testHandler = new TestHandler();
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => $key
        ], (new Logger('test'))->pushHandler($testHandler));
        $extractor->extract($this->path);

        $this->assertTrue($testHandler->hasInfo("Downloaded 0 file(s)"));
        $this->assertCount(1, $testHandler->getRecords());
    }

    /**
     * @return array
     */
    public function initialForwardSlashProvider()
    {
        return [[true], [false]];
    }
}
