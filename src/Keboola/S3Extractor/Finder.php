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
    private $subFolder;

    public function __construct(Config $config, array $state, LoggerInterface $logger)
    {
        $this->config = $config;
        $this->state = $state;
        $this->logger = $logger;
        if (!empty($this->config->getSaveAs())) {
            $this->subFolder = $this->config->getSaveAs() . '/';
        }
    }

    /**
     * @return S3File[]
     */
    public function listFiles(S3Client $client, string $outputPath): array
    {
        // Remove initial forwardslash
        $key = $this->config->getKey();
        if (substr($key, 0, 1) == '/') {
            $key = substr($key, 1);
        }

        $this->logger->info('Listing files to be downloaded');

        // Detect wildcard at the end
        /** @var S3File[] $filesToDownload */
        $filesToDownload = [];
        if (substr($key, -1) == '*') {
            $filesToDownload = $this->listWildcard($client, $outputPath, $key);
        } else {
            $filesToDownload = $this->listSingleFile($client, $outputPath, $key);
        }

        // Timestamp of last downloaded file, processed files in the last timestamp second
        $lastDownloadedFileTimestamp = isset($this->state['lastDownloadedFileTimestamp']) ? (int)$this->state['lastDownloadedFileTimestamp'] : 0;
        $processedFilesInLastTimestampSecond = isset($this->state['processedFilesInLastTimestampSecond']) ? $this->state['processedFilesInLastTimestampSecond'] : [];

        $this->logger->info(sprintf(
            'Found %s file(s)',
            count($filesToDownload)
        ));

        // Filter out old files with newFilesOnly flag
        if ($this->config->isNewFilesOnly() === true) {
            $filesToDownload = array_filter($filesToDownload, function (S3File $fileToDownload) use (
                $lastDownloadedFileTimestamp,
                $processedFilesInLastTimestampSecond
            ) {
                if ($fileToDownload->getTimestamp() < $lastDownloadedFileTimestamp) {
                    return false;
                }
                if ($fileToDownload->getTimestamp() === $lastDownloadedFileTimestamp
                    && in_array($fileToDownload->getKey(), $processedFilesInLastTimestampSecond)
                ) {
                    return false;
                }
                return true;
            });

            $this->logger->info(sprintf(
                'There are %s new file(s)',
                count($filesToDownload)
            ));
        }

        // Sort files to download using timestamp
        usort($filesToDownload, function (S3File $a, S3File $b) {
            if ($a->getTimestamp() - $b->getTimestamp() === 0) {
                return strcmp($a->getKey(), $b->getKey());
            }
            return $a->getTimestamp() - $b->getTimestamp();
        });

        // Apply limit if set
        if ($this->config->getLimit() > 0 && count($filesToDownload) > $this->config->getLimit()) {
            $this->logger->info("Downloading only {$this->config->getLimit()} oldest file(s) out of " . count($filesToDownload));
            $filesToDownload = array_slice($filesToDownload, 0, $this->config->getLimit());
        }

        return $filesToDownload;
    }

    /**
     * @return S3File[]
     */
    private function listWildcard(S3Client $client, string $outputPath, string $key)
    {
        $paginator = $client->getPaginator(
            'ListObjectsV2',
            [
                'Bucket' => $this->config->getBucket(),
                'Prefix' => substr($key, 0, -1),
                'MaxKeys' => self::MAX_OBJECTS_PER_PAGE
            ]
        );

        $filesListedCount = 0;
        $filesToDownloadCount = 0;
        /** @var S3File[] $filesToDownload */
        $filesToDownload = [];
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
                if (strlen($key) > strlen($object['Key'])) {
                    continue;
                }

                // Skip objects in subfolders if not includeSubfolders
                if (strrpos($object['Key'], '/', strlen($key) - 1) !== false && !$this->config->isIncludeSubfolders()) {
                    continue;
                }

                // Skip empty folder files (https://github.com/keboola/aws-s3-extractor/issues/21)
                if (substr($object['Key'], -1, 1) === '/') {
                    continue;
                }

                // remove wilcard mask from search key
                $keyWithoutWildcard = trim($key, "*");

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
                    $dst = $outputPath . '/' . $this->subFolder . $flattened;
                } else {
                    $dst = $outputPath . '/' . $this->subFolder . basename($object['Key']);
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
    private function listSingleFile(S3Client $client, string $outputPath, string $key)
    {
        if ($this->config->isIncludeSubfolders() === true) {
            throw new UserException("Cannot include subfolders without wildcard.");
        }

        /** @var array{
         *     ContentLength: string,
         *     LastModified: \DateTimeInterface,
         * } $head
         */
        $head = $client->getObject([
            'Bucket' => $this->config->getBucket(),
            'Key' => $key,
        ]);
        $dst = $outputPath . '/' . $this->subFolder . basename($key);
        return [
            new S3File(
                $this->config->getBucket(),
                $key,
                $head['LastModified'],
                (int)$head['ContentLength'],
                $dst
            )
        ];
    }
}
