<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Keboola\Component\UserException;
use Psr\Log\LoggerInterface;

class Finder
{
    private const MAX_OBJECTS_PER_PAGE = 1000;

    /** @var Config */
    private $config;

    /** @var LoggerInterface */
    private $logger;

    /** @var S3Client */
    private $client;

    /** @var string */
    private $key;

    /** @var string */
    private $subFolder;

    /** @var State */
    private $oldState;

    /** @var State */
    private $newState;

    /**
     * Count of all files returned by the API.
     * @var int
     */
    private $listedCount;

    /**
     * Count of all not ignored files (see isFileIgnored method).
     * @var int
     */
    private $matchedCount;

    /**
     * Count of all new files (see isFileOld method).
     * If newFilesOnly=false, the the value is equal to $matchedCount.
     * @var int
     */
    private $newCount;

    /** @var int */
    private $downloadSizeBytes;

    public function __construct(Config $config, array $state, LoggerInterface $logger, S3Client $client)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->client = $client;
        $this->key = $config->getKey();
        if (!empty($this->config->getSaveAs())) {
            $this->subFolder = $this->config->getSaveAs() . '/';
        }
        $this->oldState = new State($state);
        $this->newState = new State($state);
    }

    public function listFiles(): FinderResult
    {
        $this->listedCount = 0;
        $this->matchedCount = 0;
        $this->newCount = 0;
        $this->logger->info('Listing files to be downloaded');
        $iterator = $this->sortByTimestamp($this->listAllFiles());
        return new FinderResult($iterator, $this->newCount, $this->downloadSizeBytes, $this->newState);
    }


        $files = [];
        foreach ($this->listAllFiles() as $file) {
            $files[] = $file;
        }

        $this->logger->info(sprintf('Found %s file(s)', $this->matchedCount));
        if ($this->config->isNewFilesOnly()) {
            $this->logger->info(sprintf('There are %s new file(s)', $this->newCount));
        }

        return $this->limitCount($this->sortByTimestamp($files));
    }

    private function sortByTimestamp(array $files): array
    {
        // Sort files to download using timestamp
        usort($files, function (S3File $a, S3File $b) {
            if ($a->getTimestamp() - $b->getTimestamp() === 0) {
                return strcmp($a->getKey(), $b->getKey());
            }
            return $a->getTimestamp() - $b->getTimestamp();
        });
        return $files;
    }

    private function limitCount(array $files): array
    {
        // Apply limit if set
        if ($this->config->getLimit() > 0 && count($files) > $this->config->getLimit()) {
            $this->logger->info("Downloading only {$this->config->getLimit()} oldest file(s) out of " . count($files));
            $files = array_slice($files, 0, $this->config->getLimit());
        }
        return $files;
    }

    /**
     * @return S3File[]
     */
    private function listAllFiles(): iterable
    {
        if (substr($this->key, -1) == '*') {
            return $this->listWildcard();
        } else {
            return $this->listSingleFile();
        }
    }

    /**
     * @return S3File[]
     */
    private function listWildcard(): iterable
    {
        $keyWithoutWildcard = trim($this->key, "*");

        // search key can contain folder
        $dirPrefixToBeRemoved = '';
        if (strrpos($keyWithoutWildcard, '/') !== false) {
            $dirPrefixToBeRemoved = substr($keyWithoutWildcard, 0, strrpos($keyWithoutWildcard, '/'));
        }

        $paginator = $this->client->getPaginator(
            'ListObjectsV2',
            [
                'Bucket' => $this->config->getBucket(),
                'Prefix' => $keyWithoutWildcard,
                'MaxKeys' => self::MAX_OBJECTS_PER_PAGE
            ]
        );

        /** @var array{Contents: ?array} $page */
        foreach ($paginator as $page) {
            $objects = $page['Contents'] ?? [];

            /** @var array{StorageClass: string, Key: string, Size: string, LastModified: \DateTimeInterface} $object */
            foreach ($objects as $object) {
                $this->listedCount++;
                if ($this->isFileIgnored($object)) {
                    continue;
                }
                $this->matchedCount++;

                // remove folder mask from object key to figure out, if there is a subfolder
                $objectKeyWithoutDirPrefix = substr($object['Key'], strlen($dirPrefixToBeRemoved));

                // trim object key without dir and figure out the dir name
                $dstDir = trim(dirname($objectKeyWithoutDirPrefix), '/');

                // complete path
                if ($this->config->isIncludeSubfolders()) {
                    if ($dstDir && $dstDir != '.') {
                        $flattened = str_replace(
                            '/',
                            '-',
                            str_replace('-', '--', $dstDir . '/' . basename($object['Key']))
                        );
                    } else {
                        $flattened = str_replace(
                            '/',
                            '-',
                            str_replace('-', '--', basename($object['Key']))
                        );
                    }
                    $dst = $this->subFolder . $flattened;
                } else {
                    $dst = $this->subFolder . basename($object['Key']);
                }

                // create object
                $file = new S3File(
                    $this->config->getBucket(),
                    $object['Key'],
                    $object['LastModified'],
                    (int)$object['Size'],
                    $dst
                );

                // skip old files
                if ($this->isFileOld($file)) {
                    continue;
                }

                // update state
                $this->newCount++;
                $this->downloadSizeBytes += $file->getSizeBytes();
                if ($this->newState->lastDownloadedFileTimestamp != $file->getTimestamp()) {
                    $this->newState->processedFilesInLastTimestampSecond = [];
                }
                $this->newState->lastDownloadedFileTimestamp = max($this->newState->lastDownloadedFileTimestamp, $file->getTimestamp());
                $this->newState->processedFilesInLastTimestampSecond[] = $file->getKey();

                // log progress
                if ($this->listedCount !== 0 &&
                    $this->matchedCount !== 0 &&
                    (($this->listedCount % 10000) === 0 || ($this->matchedCount % 1000) === 0)
                ) {
                    $this->logger->info(sprintf(
                        'Listed %s files (%s matching the filter so far)',
                        $this->listedCount,
                        $this->matchedCount
                    ));
                }

                yield $file;
            }
        }
    }

    /**
     * @return S3File[]
     */
    private function listSingleFile(): iterable
    {
        if ($this->config->isIncludeSubfolders()) {
            throw new UserException("Cannot include subfolders without wildcard.");
        }

        /** @var array{ContentLength: string, LastModified: \DateTimeInterface} $head */
        $head = $this->client->getObject([
            'Bucket' => $this->config->getBucket(),
            'Key' => $this->key,
        ]);

        $this->listedCount++;
        $this->matchedCount++;
        $file = new S3File(
            $this->config->getBucket(),
            $this->key,
            $head['LastModified'],
            (int)$head['ContentLength'],
            $this->subFolder . basename($this->key)
        );

        if ($this->isFileOld($file)) {
            return;
        }

        $this->newCount++;
        yield $file;
    }

    /**
     * @param array{StorageClass: string, Key: string} $object
     */
    private function isFileIgnored(array $object): bool
    {
        // Skip objects in Glacier
        if ($object['StorageClass'] === "GLACIER") {
            return true;
        }

        // Skip folder object keys (/myfolder/) from folder wildcards (/myfolder/*) - happens with empty folder
        // https://github.com/keboola/s3-extractor/issues/1
        if (strlen($this->key) > strlen($object['Key'])) {
            return true;
        }

        // Skip objects in subfolders if not includeSubfolders
        if (strrpos($object['Key'], '/', strlen($this->key) - 1) !== false && !$this->config->isIncludeSubfolders()) {
            return true;
        }

        // Skip empty folder files (https://github.com/keboola/aws-s3-extractor/issues/21)
        if (substr($object['Key'], -1, 1) === '/') {
            return true;
        }

        return false;
    }

    private function isFileOld(S3File $file): bool
    {
        if ($this->config->isNewFilesOnly()) {
            if ($file->getTimestamp() < $this->oldState->lastDownloadedFileTimestamp) {
                return true;
            }

            if ($file->getTimestamp() === $this->oldState->lastDownloadedFileTimestamp
                && in_array($file->getKey(), $this->oldState->processedFilesInLastTimestampSecond)
            ) {
                return true;
            }
        }

        return false;
    }
}
