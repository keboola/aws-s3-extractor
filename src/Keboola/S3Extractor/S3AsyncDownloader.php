<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Aws\CommandInterface;
use Aws\CommandPool;
use Aws\ResultInterface;
use Aws\Exception\AwsException;
use Psr\Log\LoggerInterface;
use Retry\RetryProxy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\BackOff\ExponentialBackOffPolicy;
use GuzzleHttp\Psr7\LazyOpenStream;
use function Keboola\Utils\formatBytes;

class S3AsyncDownloader
{
    private const MAX_ATTEMPTS = 5;
    private const INTERVAL_MS = 500;
    private const MAX_CONCURRENT_DOWNLOADS = 50;

    /**
     * @var S3Client
     */
    private $client;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var CommandInterface[]
     */
    private $commands = [];

    /**
     * @var array
     */
    private $keys = [];

    /**
     * @var int
     */
    private $downloadedSize = 0;

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
     * @return void
     */
    public function addFileRequest(array $fileParameters): void
    {
        $this->commands[] = $this->client->getCommand('getObject', $fileParameters);
    }

    /**
     * @return void
     */
    public function processRequests(): void
    {
        (new RetryProxy(
            new SimpleRetryPolicy(self::MAX_ATTEMPTS),
            new ExponentialBackOffPolicy(self::INTERVAL_MS),
            $this->logger
        ))->call(function () {
            $this->makeCommandPool()
                ->promise()
                ->then(function () {
                    $this->logger->info(sprintf(
                        'Downloaded %d file(s) (%s)',
                        count($this->commands),
                        formatBytes($this->downloadedSize)
                    ));
                })
                ->wait();
        });
    }

    /**
     * @return CommandPool
     */
    private function makeCommandPool(): CommandPool
    {
        return new CommandPool($this->client, $this->commands, [
            'concurrency' => self::MAX_CONCURRENT_DOWNLOADS,
            'before' => function (CommandInterface $cmd, int $iterKey) {
                $this->keys[$iterKey] = $cmd->offsetGet('Key');
            },
            'fulfilled' => function (ResultInterface $result, int $iterKey) {
                $body = $result->get('Body');
                /** @var LazyOpenStream $body */
                $fileSize = $body->getSize();
                $this->logger->info(sprintf(
                    'Downloaded file complete /%s (%s)',
                    $this->keys[$iterKey],
                    formatBytes($body->getSize())
                ));

                $this->downloadedSize += $fileSize;
            },
            'rejected' => static function (AwsException $reason) {
                throw $reason;
            },
        ]);
    }
}
