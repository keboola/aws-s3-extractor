<?php

namespace Keboola\S3Extractor;

use Aws\Exception\AwsException;
use Aws\S3\S3ClientInterface;
use GuzzleHttp\Exception\ConnectException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

/**
 * Resolves the AWS region of the configured bucket.
 *
 * The lookup is a single HeadBucket call made while the retry-enabled S3 client is still
 * being built, so - unlike every later S3 call - it had no retry of its own. A purely
 * network-level blip there (e.g. cURL 35 "Recv failure: Connection reset by peer") aborted
 * the whole job with an opaque internal error.
 *
 * Only network-level failures are retried, with an exponential backoff. Every other
 * exception (403, 404, ...) propagates immediately and unchanged, and a network failure
 * that survives all attempts is rethrown as well - a persistent outage still fails loudly.
 */
class BucketRegionResolver
{
    public const MAX_ATTEMPTS = 5;
    public const INTERVAL_MS = 500;
    public const DEFAULT_REGION = 'us-east-1';

    /** @var LoggerInterface */
    private $logger;

    public function __construct(?LoggerInterface $logger = null)
    {
        $this->logger = $logger ?: new NullLogger();
    }

    public function resolve(S3ClientInterface $client, string $bucket): string
    {
        $retryProxy = new RetryProxy(
            new CallableRetryPolicy(
                function (Throwable $e): bool {
                    return self::isConnectionError($e);
                },
                self::MAX_ATTEMPTS
            ),
            new ExponentialBackOffPolicy(self::INTERVAL_MS),
            $this->logger
        );

        /** @var string|null $region */
        $region = $retryProxy->call(function () use ($client, $bucket) {
            return $client->determineBucketRegion($bucket);
        });

        return $region ?: self::DEFAULT_REGION;
    }

    /**
     * True only for failures that never reached (or never got a reply from) the S3 endpoint.
     */
    public static function isConnectionError(Throwable $e): bool
    {
        for ($current = $e; $current !== null; $current = $current->getPrevious()) {
            if ($current instanceof ConnectException) {
                return true;
            }
            if ($current instanceof AwsException && $current->isConnectionError()) {
                return true;
            }
        }

        return false;
    }
}
