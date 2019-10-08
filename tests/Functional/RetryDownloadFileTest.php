<?php

namespace Keboola\S3ExtractorTest\Functional;

use Aws\Command;
use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Keboola\S3Extractor\DownloadFile;
use PHPUnit\Framework\MockObject\MockObject;
use GuzzleHttp\Promise\Promise;

class RetryDownloadFileTest extends FunctionalTestCase
{
    public function testRetrySuccess(): void
    {
        $consecutive = 0;
        $client = $this->mockS3Client();
        $client->method('getObjectAsync')
            ->willReturnCallback(static function ($args) use (&$consecutive) {
                $consecutive++;
                if ($consecutive < 3) {
                    throw new S3Exception(
                        'Error executing "GetObject" on "foo"; AWS HTTP error: Server error: `GET bar` ' .
                        'resulted in a `503 Slow Down` response:',
                        new Command('dummy')
                    );
                }

                file_put_contents($args['SaveAs'], 'dummy content');

                return new Promise();
            });

        $handler = new TestHandler;
        /** @var S3Client $client */
        DownloadFile::process($client, (new Logger('s3ClientTest'))->pushHandler($handler), [
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => 'file1.csv',
            'SaveAs' => '/file1.csv',
        ]);

        $expectedFile = '/file1.csv';
        self::assertFileExists($expectedFile);
        self::assertEquals('dummy content', (string) file_get_contents($expectedFile));
        self::assertTrue($handler->hasInfoThatContains('resulted in a `503 Slow Down` response'));
        self::assertTrue($handler->hasInfoThatContains('Retrying'));
    }

    public function testRetryFailure(): void
    {
        $client = $this->mockS3Client();
        $client->method('getObjectAsync')
            ->willReturnCallback(static function () {
                throw new S3Exception(
                    'Error executing "GetObject" on "foo"; AWS HTTP error: Server error: `GET bar` ' .
                    'resulted in a `503 Slow Down` response:',
                    new Command('dummy')
                );
            });

        $this->expectException(S3Exception::class);
        $this->expectExceptionMessage('resulted in a `503 Slow Down` response');

        /** @var S3Client $client */
        DownloadFile::process($client, (new Logger('s3ClientTest'))->pushHandler(new TestHandler), [
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => 'file1.csv',
            'SaveAs' => '/file1.csv',
        ]);
    }

    private function mockS3Client(): MockObject
    {
        return $this->getMockBuilder(S3Client::class)
            ->setMethods(['getObjectAsync'])
            ->disableOriginalConstructor()
            ->getMock();
    }
}
