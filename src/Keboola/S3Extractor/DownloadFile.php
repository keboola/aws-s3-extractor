<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Psr\Log\LoggerInterface;
use Retry\RetryProxy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\BackOff\ExponentialBackOffPolicy;
use GuzzleHttp\Promise\Promise;

class DownloadFile
{
    private const MAX_ATTEMPTS = 5;
    private const INTERVAL_MS = 500;

    /**
     * @param S3Client $client
     * @param LoggerInterface $logger
     * @param array $fileParameters
     * @return Promise
     */
    public static function process(S3Client $client, LoggerInterface $logger, array $fileParameters): Promise
    {
        return (new RetryProxy(
            new SimpleRetryPolicy(self::MAX_ATTEMPTS),
            new ExponentialBackOffPolicy(self::INTERVAL_MS),
            $logger
        ))->call(static function () use ($client, $fileParameters) {
            return $client->getObjectAsync($fileParameters);
        });
    }
}
