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
use GuzzleHttp\Promise\PromiseInterface;
use function Keboola\Utils\formatBytes;

class S3AsyncDownloader
{
    private const MAX_ATTEMPTS = 5;
    private const INTERVAL_MS = 500;
    private const MAX_CONCURRENT_DOWNLOADS = 50;
    private const FORCE_GC_MIN_BYTES = 512 * 1024 * 1024; // 512MiB

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
    private $filesParameter = [];

    /**
     * @var int
     */
    private $downloadedSize = 0;

    /**
     * @var callable|null
     */
    private $retryCallback;

    /**
     * @param S3Client $client
     * @param LoggerInterface $logger
     * @param callable|null $retryCallback
     */
    public function __construct(S3Client $client, LoggerInterface $logger, callable $retryCallback = null)
    {
        $this->client = $client;
        $this->logger = $logger;
        $this->retryCallback = $retryCallback;
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
        $this->makeCommandPool()
            ->promise()
            ->wait();

        $this->logger->info(sprintf(
            'Downloaded %d file(s) (%s)',
            count($this->commands),
            formatBytes($this->downloadedSize)
        ));
    }

    /**
     * @return CommandPool
     */
    private function makeCommandPool(): CommandPool
    {
        return new CommandPool($this->client, $this->commands, [
            'concurrency' => self::MAX_CONCURRENT_DOWNLOADS,
            'before' => function (CommandInterface $command, int $index) {
                // https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_commands.html#manual-garbage-collection-between-commands
                if (memory_get_usage() >= self::FORCE_GC_MIN_BYTES) {
                    gc_collect_cycles();
                }
                $this->filesParameter[$index] = $command->toArray();
            },
            'fulfilled' => function (ResultInterface $result, int $index) {
                $this->processFulfilled($result, $index);
            },
            'rejected' => function (AwsException $reason, int $index, PromiseInterface $promise) {
                /** @var ResultInterface $result */
                $result = (new RetryProxy(
                    new SimpleRetryPolicy(self::MAX_ATTEMPTS),
                    new ExponentialBackOffPolicy(self::INTERVAL_MS),
                    $this->logger
                ))->call(function () use ($index) {
                    if (is_callable($this->retryCallback)) {
                        call_user_func($this->retryCallback, $this->filesParameter[$index]);
                    }
                    return $this->client->getObjectAsync($this->filesParameter[$index])->wait();
                });
                $promise->then(function () use ($result, $index) {
                    $this->processFulfilled($result, $index);
                });
                $promise->resolve($result);
            },
        ]);
    }

    /**
     * @param ResultInterface $result
     * @param int $index
     */
    private function processFulfilled(ResultInterface $result, int $index): void
    {
        $body = $result->get('Body');
        /** @var LazyOpenStream $body */
        $fileSize = $body->getSize();
        $this->logger->info(sprintf(
            'Downloaded file /%s (%s)',
            $this->filesParameter[$index]['Key'],
            formatBytes($fileSize)
        ));

        $this->downloadedSize += $fileSize;
    }
}
