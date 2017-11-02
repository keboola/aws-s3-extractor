<?php
namespace Keboola\S3ExtractorTest;

use Keboola\S3Extractor\Exception;
use Keboola\S3Extractor\Extractor;
use PHPUnit\Framework\TestCase;

class ExceptionsTest extends TestCase
{
    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';

    protected $path = '/tmp/errors';

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

    public function testInvalidBucket()
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Bucket " . getenv(self::AWS_S3_BUCKET_ENV) . "_invalid" . " not found.");
        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV) . "_invalid",
            "key" => "/file1.csv"
        ]);
        $extractor->extract($this->path);
    }

    public function testInvalidCredentials()
    {

        $this->expectException(Exception::class);
        $this->expectExceptionMessage("Invalid credentials or permissions not set correctly. Did you set s3:GetBucketLocation?");

        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV) . "_invalid",
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => "/file1.csv"
        ]);
        $extractor->extract($this->path);
    }

    public function testInvalidKey()
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage("File doesnotexist not found.");

        $extractor = new Extractor([
            "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
            "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
            "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
            "key" => "/doesnotexist"
        ]);
        $extractor->extract($this->path);
    }
}
