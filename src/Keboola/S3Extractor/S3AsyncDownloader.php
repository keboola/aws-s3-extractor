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
use Symfony\Component\Filesystem\Filesystem;
use function iter\makeRewindable;
use function Keboola\Utils\formatBytes;

class S3AsyncDownloader
{
    private const MAX_ATTEMPTS = 5;
    private const INTERVAL_MS = 500;
    private const MAX_CONCURRENT_DOWNLOADS = 200;

    /** @var S3Client */
    private $client;

    /** @var Filesystem */
    private $fs;

    /** @var LoggerInterface */
    private $logger;

    /** @var string */
    private $outputDir;

    /** @var State */
    private $state;

    /** @var \Iterator|File[] */
    private $files;

    /** @var array */
    private $filesParameter = [];

    /** @var int */
    private $downloadedCount = 0;

    /** @var int */
    private $downloadedSize = 0;

    /** @var callable|null */
    private $retryCallback;

    /**
     * @param \Iterator|File[] $files
     */
    public function __construct(
        S3Client        $client,
        LoggerInterface $logger,
        State           $state,
        string          $outputDir,
        \Iterator       $files,
        callable        $retryCallback = null
    ) {
        $this->client = $client;
        $this->fs = new Filesystem();
        $this->logger = $logger;
        $this->state = $state;
        $this->outputDir = $outputDir;
        $this->files = $files;
        $this->retryCallback = $retryCallback;
    }

    public function startAndWait(): void
    {
        $this->makeCommandPool()
            ->promise()
            ->wait();

        $this->logger->info(sprintf(
            'Downloaded %d file(s) (%s)',
            $this->downloadedCount,
            formatBytes($this->downloadedSize)
        ));
    }

    private function makeCommandPool(): CommandPool
    {
        return new CommandPool($this->client, $this->getCommands(), [
            'concurrency' => self::MAX_CONCURRENT_DOWNLOADS,
            'before' => function (CommandInterface $command, int $index) {
                $parameters = $command->toArray();

                // create folder
                if (!$this->fs->exists(dirname($parameters['SaveAs']))) {
                    $this->fs->mkdir(dirname($parameters['SaveAs']));
                }

                $this->filesParameter[$index] = $parameters;
            },
            'fulfilled' => function (ResultInterface $result, int $index) {
                if (($index % 10000) === 0) {
                    gc_collect_cycles();
                }
                $this->processFulfilled($result, $index);
                unset($this->filesParameter[$index]);
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
     * @return \Iterator|CommandInterface[]
     */
    private function getCommands(): \Iterator
    {
        return makeRewindable(function (): \Iterator {
            foreach ($this->files as $file) {
                // Update the state of the incremental fetching
                if ($this->state->lastTimestamp != $file->getTimestamp()) {
                    $this->state->filesInLastTimestamp = [];
                }
                $this->state->lastTimestamp = max($this->state->lastTimestamp, $file->getTimestamp());
                $this->state->filesInLastTimestamp[] = $file->getKey();

                // Create command
                yield $this->client->getCommand('getObject', $file->getParameters($this->outputDir));
            }
        })();
    }

    private function processFulfilled(ResultInterface $result, int $index): void
    {
        /** @var LazyOpenStream $body */
        $body = $result->get('Body');
        $fileSize = $body->getSize();
        $this->logger->info(sprintf(
            'Downloaded file /%s (%s)',
            $this->filesParameter[$index]['Key'],
            formatBytes($fileSize)
        ));

        $this->downloadedCount++;
        $this->downloadedSize += $fileSize;
    }
}
