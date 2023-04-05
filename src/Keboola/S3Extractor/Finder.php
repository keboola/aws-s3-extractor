<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Keboola\Component\UserException;
use Psr\Log\LoggerInterface;

/**
 * Finder search for file for download.
 */
class Finder
{
    private const MAX_OBJECTS_PER_PAGE = 100;

    /** @var LoggerInterface */
    private $logger;

    /** @var S3Client */
    private $client;

    /** @var string */
    private $bucket;

    /** @var string */
    private $key;

    /** @var bool */
    private $newFilesOnly;

    /** @var bool */
    private $includeSubFolders;

    /** @var string */
    private $subFolder;

    /** @var int */
    private $limit;

    /** @var State */
    private $state;

    public function __construct(Config $config, State $state, LoggerInterface $logger, S3Client $client)
    {
        $this->logger = $logger;
        $this->client = $client;
        $this->bucket = $config->getBucket();
        $this->key = $config->getKey();
        $this->newFilesOnly = $config->isNewFilesOnly();
        $this->includeSubFolders = $config->isIncludeSubfolders();
        if (!empty($config->getSaveAs())) {
            $this->subFolder = $config->getSaveAs() . '/';
        }
        $this->limit = $config->getLimit();
        $this->state = $state;
    }

    /**
     * @return iterable|File[]
     */
    public function findFiles(): array
    {
        $this->logger->info('Listing files to be downloaded');

        // Detect wildcard at the end
        if (substr($this->key, -1) == '*') {
            $filesToDownload = $this->listFilesInPrefix();
        } else {
            $filesToDownload = $this->listSingleFile();
        }

        $this->logger->info(sprintf(
            'Found %s file(s)',
            count($filesToDownload)
        ));

        $filesToDownload = $this->sort($filesToDownload);
        $filesToDownload = $this->limit($filesToDownload);
        return $filesToDownload;
    }

    /**
     * @return iterable|File[]
     */
    private function listFilesInPrefix(): array
    {
        $paginator = $this->client->getPaginator(
            'ListObjectsV2',
            [
                'Bucket' => $this->bucket,
                'Prefix' => substr($this->key, 0, -1),
                'MaxKeys' => self::MAX_OBJECTS_PER_PAGE
            ]
        );

        /** @var File[] $filesToDownload */
        $filesToDownload = [];

        $filesListedCount = 0;
        $filesToDownloadCount = 0;
        $newFilesCount = 0;
        /** @var array{
         *     Contents: ?array,
         * } $page
         */
        foreach ($paginator as $page) {
            $objects = $page['Contents'] ?? [];
            foreach ($objects as $object) {
                /** @var array{
                 *     StorageClass: string,
                 *     Key: string,
                 *     Size: string,
                 *     LastModified: \DateTimeInterface,
                 * } $object
                 */
                $filesListedCount++;

                // Skip objects in Glacier
                if ($object['StorageClass'] === "GLACIER") {
                    continue;
                }

                // Skip folder object keys (/myfolder/) from folder wildcards (/myfolder/*) - happens with empty folder
                // https://github.com/keboola/s3-extractor/issues/1
                if (strlen($this->key) > strlen($object['Key'])) {
                    continue;
                }

                // Skip objects in subfolders if not includeSubfolders
                if (strrpos($object['Key'], '/', strlen($this->key) - 1) !== false && !$this->includeSubFolders) {
                    continue;
                }

                // Skip empty folder files (https://github.com/keboola/aws-s3-extractor/issues/21)
                if (substr($object['Key'], -1, 1) === '/') {
                    continue;
                }

                // remove wilcard mask from search key
                $keyWithoutWildcard = trim($this->key, "*");

                // search key contains folder
                $dirPrefixToBeRemoved = '';
                if (strrpos($keyWithoutWildcard, '/') !== false) {
                    $dirPrefixToBeRemoved = substr($keyWithoutWildcard, 0, strrpos($keyWithoutWildcard, '/'));
                }

                // remove folder mask from object key to figure out, if there is a subfolder
                $objectKeyWithoutDirPrefix = substr($object['Key'], strlen($dirPrefixToBeRemoved));

                // trim object key without dir and figure out the dir name
                $dstDir = trim(dirname($objectKeyWithoutDirPrefix), '/');

                // complete path
                if ($this->includeSubFolders) {
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

                $file = new File($this->bucket, $object['Key'], $object['LastModified'], (int)$object['Size'], $dst);
                $filesToDownloadCount++;

                if ($this->isFileOld($file)) {
                    continue;
                }
                $newFilesCount++;

                $filesToDownload[] = $file;
                $filesToDownloadCount++;

                $isImportantMilestoneForListed = ($filesListedCount % 10000) === 0
                    && $filesListedCount !== 0;
                $isImportantMilestoneForDownloaded = ($filesToDownloadCount % 1000) === 0
                    && $filesToDownloadCount !== 0;
                if ($isImportantMilestoneForListed || $isImportantMilestoneForDownloaded) {
                    $this->logger->info(sprintf(
                        'Listed %s files (%s matching the filter so far)',
                        $filesListedCount,
                        $filesToDownloadCount
                    ));
                }
            }
        }

        if ($this->newFilesOnly) {
            $this->logger->info(sprintf(
                'There are %s new file(s)',
                count($newFilesCount)
            ));
        }
    }

    /**
     * @return iterable|File[]
     */
    private function listSingleFile(): array
    {
        if ($this->includeSubFolders) {
            throw new UserException("Cannot include subfolders without wildcard.");
        }
        $dst = $this->subFolder . basename($this->key);
        $head = $this->client->headObject([
            'Bucket' => $this->bucket,
            'Key' => $this->key,
            'SaveAs' => $dst,
        ]);
        return [new File($this->bucket, $this->key, $head['LastModified'], $head['ContentLength'], $dst)];
    }

    /**
     * @param iterable|File[] $filesToDownload
     * @return iterable|File[]
     */
    private function sort(array $filesToDownload): array
    {
        // Sort files to download using timestamp
        usort($filesToDownload, function ($a, $b) {
            if (intval($a["timestamp"]) - intval($b["timestamp"]) === 0) {
                return strcmp($a["parameters"]["Key"], $b["parameters"]["Key"]);
            }
            return intval($a["timestamp"]) - intval($b["timestamp"]);
        });
        return $filesToDownload;
    }

    /**
     * @param iterable|File[] $filesToDownload
     * @return iterable|File[]
     */
    private function limit(array $filesToDownload): array
    {
        // Apply limit if set
        if ($this->limit > 0 && count($filesToDownload) > $this->limit) {
            $this->logger->info("Downloading only {$this->limit} oldest file(s) out of " . count($filesToDownload));
            $filesToDownload = array_slice($filesToDownload, 0, $this->limit);
        }
        return $filesToDownload;
    }

    private function isFileOld(File $file): bool
    {
        if ($this->newFilesOnly) {
            if ($file->getTimestamp() < $this->state->lastTimestamp) {
                return true;
            }

            if ($file->getTimestamp() === $this->state->lastTimestamp
                && in_array($file->getKey(), $this->state->filesInLastTimestamp)
            ) {
                return true;
            }
        }

        return false;
    }
}
