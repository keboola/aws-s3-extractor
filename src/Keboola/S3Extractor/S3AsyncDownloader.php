<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Psr\Log\LoggerInterface;
use Retry\RetryProxy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\BackOff\ExponentialBackOffPolicy;
use GuzzleHttp\Promise\Promise;
use function Keboola\Utils\formatBytes;
use function GuzzleHttp\Promise\each_limit_all;

class S3AsyncDownloader
{
    private const MAX_ATTEMPTS = 5;
    private const INTERVAL_MS = 500;
    private const ASYNC_DOWNLOAD_LIMIT = 50;

    /**
     * @var S3Client
     */
    private $client;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Promise[]
     */
    private $promises;

    /**
     * @param S3Client $client
     * @param LoggerInterface $logger
     */
    public function __construct(S3Client $client, LoggerInterface $logger)
    {
        $this->client = $client;
        $this->logger = $logger;
    }

    /**
     * @param array $fileParameters
     * @param int $sizeBytes
     * @return void
     */
    public function fileRequest(array $fileParameters, int $sizeBytes): void
    {
        $this->promises[] = $this->client->getObjectAsync($fileParameters)
            ->then(
                function () use ($fileParameters, $sizeBytes) {
                    $this->logger->info(sprintf(
                        'Downloading file complete /%s (%s)',
                        $fileParameters['Key'],
                        formatBytes($sizeBytes)
                    ));
                }
            );
    }

    /**
     * @return void
     */
    public function downloadFiles(): void
    {
        (new RetryProxy(
            new SimpleRetryPolicy(self::MAX_ATTEMPTS),
            new ExponentialBackOffPolicy(self::INTERVAL_MS),
            $this->logger
        ))->call(function () {
            each_limit_all($this->promises, self::ASYNC_DOWNLOAD_LIMIT)->wait();
        });
    }
}
