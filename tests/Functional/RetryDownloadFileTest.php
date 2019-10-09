<?php

namespace Keboola\S3ExtractorTest\Functional;

use Aws\S3\Exception\S3Exception;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Keboola\S3Extractor\S3AsyncDownloader;

class RetryDownloadFileTest extends FunctionalTestCase
{
    public function testRetryFailure(): void
    {
        $handler = new TestHandler;
        $downloader = new S3AsyncDownloader(self::s3Client(), (new Logger('s3ClientTest'))->pushHandler($handler));
        $downloader->fileRequest([
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => 'file-404.csv',
            'SaveAs' => 'file-404.csv',
        ], 0);

        try {
            $downloader->downloadFiles();
        } catch (S3Exception $e) {
            $this->assertCount(4, $handler->getRecords());
            self::assertTrue($handler->hasInfoThatContains('404 Not Found'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [1x]'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [2x]'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [3x]'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [4x]'));
        }
    }
}
