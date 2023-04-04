<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Keboola\Component\UserException;
use Psr\Log\LoggerInterface;

class Finder
{
    private const MAX_OBJECTS_PER_PAGE = 1000;

    /**
     * @var Config
     */
    private $config;

    /**
     * @var array
     */
    private $state;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var string
     */
    private $key;

    /**
     * @var string
     */
    private $subFolder;

    /**
     * @var int
     */
    private $lastDownloadedFileTimestamp;

    /**
     * @var string[]
     */
    private $processedFilesInLastTimestampSecond;

    public function __construct(Config $config, array $state, LoggerInterface $logger)
    {
        $this->config = $config;
        $this->state = $state;
        $this->logger = $logger;
        $this->key = $config->getKey();
        if (!empty($this->config->getSaveAs())) {
            $this->subFolder = $this->config->getSaveAs() . '/';
        }
        $this->lastDownloadedFileTimestamp = (int)($this->state['lastDownloadedFileTimestamp'] ?? 0);
        $this->processedFilesInLastTimestampSecond = $this->state['processedFilesInLastTimestampSecond'] ?? [];
    }

    /**
     * @return S3File[]
     */
    public function listFiles(S3Client $client): array
    {
        $this->logger->info('Listing files to be downloaded');
        $files = $this->listAllFiles($client);
        $this->logger->info(sprintf('Found %s file(s)', count($files)));

        // Filter out old files with newFilesOnly flag
        if ($this->config->isNewFilesOnly() === true) {
            $files = array_filter($files, function (S3File $files) {
                if ($files->getTimestamp() < $this->lastDownloadedFileTimestamp) {
                    return false;
                }
                if ($files->getTimestamp() === $this->lastDownloadedFileTimestamp
                    && in_array($files->getKey(), $this->processedFilesInLastTimestampSecond)
                ) {
                    return false;
                }
                return true;
            });

            $this->logger->info(sprintf('There are %s new file(s)', count($files)));
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
    private function listAllFiles(S3Client $client)
    {
        if (substr($this->key, -1) == '*') {
            return $this->listWildcard($client);
        } else {
            return $this->listSingleFile($client);
        }
    }

    /**
     * @return S3File[]
     */
    private function listWildcard(S3Client $client)
    {
        $paginator = $client->getPaginator(
            'ListObjectsV2',
            [
                'Bucket' => $this->config->getBucket(),
                'Prefix' => substr($this->key, 0, -1),
                'MaxKeys' => self::MAX_OBJECTS_PER_PAGE
            ]
        );

        $filesListedCount = 0;
        $filesToDownloadCount = 0;
        /** @var S3File[] $filesToDownload */
        $filesToDownload = [];

        /** @var array{Contents: ?array} $page */
        foreach ($paginator as $page) {
            $objects = $page['Contents'] ?? [];

            /** @var array{StorageClass: string, Key: string, Size: string, LastModified: \DateTimeInterface} $object */
            foreach ($objects as $object) {
                $filesListedCount++;
                if ($this->isFileIgnored($object)) {
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
                $filesToDownload[] = new S3File(
                    $this->config->getBucket(),
                    $object['Key'],
                    $object['LastModified'],
                    (int)$object['Size'],
                    $dst
                );
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

        return $filesToDownload;
    }

    /**
     * @return S3File[]
     */
    private function listSingleFile(S3Client $client)
    {
        if ($this->config->isIncludeSubfolders()) {
            throw new UserException("Cannot include subfolders without wildcard.");
        }

        /** @var array{ContentLength: string, LastModified: \DateTimeInterface} $head */
        $head = $client->getObject([
            'Bucket' => $this->config->getBucket(),
            'Key' => $this->key,
        ]);

        return [
            new S3File(
                $this->config->getBucket(),
                $this->key,
                $head['LastModified'],
                (int)$head['ContentLength'],
                $this->subFolder . basename($this->key)
            )
        ];
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
}
