<?php

namespace Keboola\S3ExtractorTest\Unit;

use Aws\Command;
use Aws\S3\Exception\S3Exception;
use Aws\S3\S3ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use Keboola\S3Extractor\BucketRegionResolver;
use PHPUnit\Framework\TestCase;
use Throwable;

class BucketRegionResolverTest extends TestCase
{
    public function testReturnsRegionOnFirstAttempt(): void
    {
        $client = $this->createMock(S3ClientInterface::class);
        $client->expects($this->once())
            ->method('determineBucketRegion')
            ->with('my-bucket')
            ->willReturn('eu-central-1');

        /** @var S3ClientInterface $client */
        $this->assertSame('eu-central-1', (new BucketRegionResolver())->resolve($client, 'my-bucket'));
    }

    public function testFallsBackToDefaultRegionWhenLookupReturnsNothing(): void
    {
        $client = $this->createMock(S3ClientInterface::class);
        $client->expects($this->once())
            ->method('determineBucketRegion')
            ->willReturn('');

        /** @var S3ClientInterface $client */
        $this->assertSame(
            BucketRegionResolver::DEFAULT_REGION,
            (new BucketRegionResolver())->resolve($client, 'my-bucket')
        );
    }

    public function testRetriesConnectionErrorAndThenSucceeds(): void
    {
        $attempts = 0;
        $client = $this->createMock(S3ClientInterface::class);
        $client->expects($this->exactly(3))
            ->method('determineBucketRegion')
            ->willReturnCallback(function () use (&$attempts): string {
                $attempts++;
                if ($attempts < 3) {
                    throw $this->connectionError();
                }
                return 'eu-west-1';
            });

        /** @var S3ClientInterface $client */
        $this->assertSame('eu-west-1', (new BucketRegionResolver())->resolve($client, 'my-bucket'));
    }

    public function testRethrowsConnectionErrorAfterAttemptsAreExhausted(): void
    {
        $client = $this->createMock(S3ClientInterface::class);
        $client->expects($this->exactly(BucketRegionResolver::MAX_ATTEMPTS))
            ->method('determineBucketRegion')
            ->willReturnCallback(function (): string {
                throw $this->connectionError();
            });

        $this->expectException(S3Exception::class);
        $this->expectExceptionMessage('Recv failure: Connection reset by peer');

        /** @var S3ClientInterface $client */
        (new BucketRegionResolver())->resolve($client, 'my-bucket');
    }

    public function testDoesNotRetryNonConnectionError(): void
    {
        $client = $this->createMock(S3ClientInterface::class);
        $client->expects($this->once())
            ->method('determineBucketRegion')
            ->willReturnCallback(function (): string {
                throw new S3Exception(
                    'Error executing "HeadBucket"; AWS HTTP error: 403 Forbidden',
                    new Command('HeadBucket'),
                    ['code' => 'AccessDenied']
                );
            });

        $this->expectException(S3Exception::class);
        $this->expectExceptionMessage('403 Forbidden');

        /** @var S3ClientInterface $client */
        (new BucketRegionResolver())->resolve($client, 'my-bucket');
    }

    public function testIsConnectionErrorRecognisesOnlyNetworkFailures(): void
    {
        $this->assertTrue(BucketRegionResolver::isConnectionError($this->connectionError()));
        $this->assertFalse(BucketRegionResolver::isConnectionError(
            new S3Exception('403 Forbidden', new Command('HeadBucket'), ['code' => 'AccessDenied'])
        ));
        $this->assertFalse(BucketRegionResolver::isConnectionError(new \RuntimeException('boom')));
    }

    /**
     * Mirrors the observed production failure: an S3Exception wrapping a Guzzle ConnectException.
     */
    private function connectionError(): Throwable
    {
        return new S3Exception(
            'Error executing "HeadBucket"; AWS HTTP error: cURL error 35: '
            . 'Recv failure: Connection reset by peer',
            new Command('HeadBucket'),
            ['connection_error' => true],
            new ConnectException(
                'cURL error 35: Recv failure: Connection reset by peer',
                new Request('HEAD', '/')
            )
        );
    }
}
