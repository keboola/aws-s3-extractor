<?php

namespace Keboola\S3ExtractorTest\Functional;

use Aws\S3\Exception\S3Exception;
use Keboola\S3Extractor\File;
use Keboola\S3Extractor\State;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Keboola\S3Extractor\S3AsyncDownloader;
use Symfony\Component\Filesystem\Filesystem;
use function iter\makeRewindable;

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

        /** @var \Iterator|File[] $files */
        $files = makeRewindable(function () use ($tempPath, $retryFile): \Iterator {
            $bucket = getenv(self::AWS_S3_BUCKET_ENV);
            $lastModified = new \DateTimeImmutable();
            $size = 123;

            yield new File($bucket, 'file1.csv', $lastModified, $size, $tempPath . 'file1.csv');
            yield new File($bucket, 'collision-file1.csv', $lastModified, $size, $tempPath . 'collision-file1.csv');
            yield new File($bucket, $retryFile, $lastModified, $size, $tempPath . $retryFile);
        })();

        $handler = new TestHandler;
        $logger = (new Logger('s3ClientTest'))->pushHandler($handler);
        $downloader = new S3AsyncDownloader(self::s3Client(), $logger, new State([]), "", $files, $retryCallback);

        $downloader->startAndWait();

        self::assertFileExists($tempPath . 'file1.csv');
        self::assertFileExists($tempPath . 'collision-file1.csv');
        self::assertFileExists($tempPath . $retryFile);
        $this->assertCount(6, $handler->getRecords());
        self::assertTrue($handler->hasInfoThatContains('Downloaded file /file1.csv (97 B)'));
        self::assertTrue($handler->hasInfoThatContains('Downloaded file /collision-file1.csv (117 B)'));
        self::assertTrue($handler->hasInfoThatContains('Retrying... [1x]'));
        self::assertTrue($handler->hasInfoThatContains('Retrying... [2x]'));
        self::assertTrue($handler->hasInfoThatContains('Downloaded file /retry-file.csv (2 B)'));
        self::assertTrue($handler->hasInfoThatContains('Downloaded 3 file(s) (216 B)'));
    }

    public function testRetryFailure(): void
    {
        /** @var \Iterator|File[] $files */
        $files = makeRewindable(function (): \Iterator {
            $bucket = getenv(self::AWS_S3_BUCKET_ENV);
            $key = 'file-not-found.csv';
            $lastModified = new \DateTimeImmutable();
            $size = 123;
            $destination = self::makeTempPath('retry-failure') . 'file-not-found.csv';
            yield new File($bucket, $key, $lastModified, $size, $destination);
        })();

        $handler = new TestHandler;
        $logger = (new Logger('s3ClientTest'))->pushHandler($handler);
        $downloader = new S3AsyncDownloader(self::s3Client(), $logger, new State([]), "", $files);

        try {
            $downloader->startAndWait();
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
