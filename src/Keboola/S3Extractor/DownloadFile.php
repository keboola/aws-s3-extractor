<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Monolog\Logger;
use Retry\RetryProxy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\BackOff\ExponentialBackOffPolicy;

class DownloadFile
{
    private const MAX_ATTEMPTS = 5;
    private const INTERVAL_MS = 500;

    /**
     * @param S3Client $client
     * @param Logger $logger
     * @param array $fileParameters
     */
    public static function process(S3Client $client, Logger $logger, array $fileParameters): void
    {
        (new RetryProxy(
            new SimpleRetryPolicy(self::MAX_ATTEMPTS),
            new ExponentialBackOffPolicy(self::INTERVAL_MS),
            $logger
        ))->call(static function () use ($client, $fileParameters) {
            $client->getObject($fileParameters);
        });
    }
}
