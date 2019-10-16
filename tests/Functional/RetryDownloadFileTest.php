<?php

namespace Keboola\S3ExtractorTest\Functional;

use Aws\S3\Exception\S3Exception;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Keboola\S3Extractor\S3AsyncDownloader;

class RetryDownloadFileTest extends FunctionalTestCase
{
    public function testRetrySuccess(): void
    {
        $retryFile = 'retry-file.csv';
        self::s3Client()->deleteObject([
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => $retryFile,
        ]);

        $handler = new TestHandler;
        $tempPath = self::makeTempPath('retry-success');
        $consecutive = 0;

        $retryCallback = static function (array $fileParameters) use (&$consecutive, $tempPath) {
            $consecutive++;
            if ($consecutive === 3) {
                self::s3Client()->putObject([
                    'Bucket' => $fileParameters['Bucket'],
                    'Key' => $fileParameters['Key'],
                    'Body' => file_put_contents($tempPath . $fileParameters['Key'], 'dummy content'),
                ]);
            }
        };

        $downloader = new S3AsyncDownloader(
            self::s3Client(),
            (new Logger('s3ClientTest'))->pushHandler($handler),
            $retryCallback
        );

        $downloader->addFileRequest([
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => 'file1.csv',
            'SaveAs' => $tempPath . 'file1.csv',
        ]);

        $downloader->addFileRequest([
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => 'collision-file1.csv',
            'SaveAs' => $tempPath . 'collision-file1.csv',
        ]);

        $downloader->addFileRequest([
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => $retryFile,
            'SaveAs' => $tempPath . $retryFile,
        ]);

        $downloader->processRequests();

        self::assertFileExists($tempPath . 'file1.csv');
        self::assertFileExists($tempPath . 'collision-file1.csv');
        self::assertFileExists($tempPath . $retryFile);
        $this->assertCount(6, $handler->getRecords());
        self::assertTrue($handler->hasInfoThatContains('Downloaded file complete /file1.csv (97 B)'));
        self::assertTrue($handler->hasInfoThatContains('Downloaded file complete /collision-file1.csv (117 B)'));
        self::assertTrue($handler->hasInfoThatContains('Retrying... [1x]'));
        self::assertTrue($handler->hasInfoThatContains('Retrying... [2x]'));
        self::assertTrue($handler->hasInfoThatContains('Downloaded file complete /retry-file.csv (2 B)'));
        self::assertTrue($handler->hasInfoThatContains('Downloaded 3 file(s) (216 B)'));
    }

    public function testRetryFailure(): void
    {
        $handler = new TestHandler;
        $downloader = new S3AsyncDownloader(self::s3Client(), (new Logger('s3ClientTest'))->pushHandler($handler));
        $downloader->addFileRequest([
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => 'file-not-found.csv',
            'SaveAs' => self::makeTempPath('retry-failure') . 'file-not-found.csv',
        ]);

        try {
            $downloader->processRequests();
        } catch (S3Exception $e) {
            $this->assertCount(4, $handler->getRecords());
            self::assertTrue($handler->hasInfoThatContains('404 Not Found'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [1x]'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [2x]'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [3x]'));
            self::assertTrue($handler->hasInfoThatContains('Retrying... [4x]'));
        }
    }

    /**
     * @param string $testDirectory
     * @return string
     */
    public static function makeTempPath(string $testDirectory): string
    {
        $path = sprintf('/tmp/%s/', uniqid($testDirectory, true));
        mkdir($path, 0777, true);
        return $path;
    }
}
