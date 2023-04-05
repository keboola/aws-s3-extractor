<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Keboola\Component\UserException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;
use function iter\makeRewindable;

/**
 * Finder search for file for download.
 */
class Finder
{
    private const MAX_OBJECTS_PER_PAGE = 1000;
    private const TIMESTAMP_STR_LENGTH = 12;

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

    public function findFiles(): FinderResult
    {
        $tmpFilePath = $this->dumpFilesListToTmpFile($this->listFiles());
        $sortedFilePath = $this->sortLines($tmpFilePath);

        // Make rewindable iterator, so files can be iterated multiple times if needed.
        $iterator = makeRewindable(function () use ($sortedFilePath): \Iterator {
            return $this->iteratorFromTmpFile($sortedFilePath);
        })();

        // Compute total count and size, take into account the limit.
        // It is needed for a log message before download.
        $count = 0;
        $size = 0;
        foreach ($iterator as $file) {
            $count++;
            $size += $file->getSizeBytes();
        }
        return new FinderResult($iterator, $count, $size);
    }

    /**
     * @return   \Iterator|File[] $files
     */
    private function iteratorFromTmpFile(string $sortedFilePath): \Iterator
    {
        // Read sorted metadata
        $sortedFile = fopen($sortedFilePath, "r");
        if (!$sortedFile) {
            throw new \RuntimeException(sprintf('Cannot open sorted file "%s".', $sortedFilePath));
        }
        try {
            $i = 0;
            while (($line = fgets($sortedFile, 20480)) !== false) {
                // Skip the first word "<timestamp><key>", separated by NUL char
                strtok($line, "\0");
                $serialized = (string)strtok("");

                /** @var File $file */
                $file = unserialize(base64_decode($serialized));

                yield $file;

                if ($this->limit > 0 && ++$i >= $this->limit) {
                    break;
                }
            }
        } finally {
            fclose($sortedFile);
        }
    }

    /**
     * sortLines by the first word: "<timestamp><key>"
     */
    private function sortLines(string $tmpFilePath): string
    {
        $sortedFilePath = $tmpFilePath . ".sorted";
        $args = [
            "sort", // the sort command has small memory requirements, it uses temp files for sorting, instead of memory
            "--stable",
            "--output", $sortedFilePath,
            "-t", '\0', // words are separated by NUL character
            "-k", "1,1", // sort lines by the first word "<timestamp><key>" (start=1, end=1)
            "--parallel=1", // optimizes memory usage
            $tmpFilePath,
        ];
        $env = ["LC_ALL" => "C"];
        (new Process($args, null, $env))->mustRun();
        return $sortedFilePath;
    }

    /**
     * @param \Iterator|File[] $files
     */
    private function dumpFilesListToTmpFile(\Iterator $files): string
    {
        // Write all metadata from the generator to a temporary file, to prevent memory issues.
        $tmpFilePath = tempnam(sys_get_temp_dir(), 'aws-files');
        if (!$tmpFilePath) {
            throw new \RuntimeException("Cannot create a temp file.");
        }

        $tmpFile = fopen($tmpFilePath, "w");
        if (!$tmpFile) {
            throw new \RuntimeException(sprintf('Cannot open temp file "%s".', $tmpFilePath));
        }

        try {
            foreach ($files as $file) {
                // Write each file metadata as: <timestamp><key>\0<serialized S3File object>\n
                // The base64 encoding is used to prevent new lines in the serialized object.
                fwrite($tmpFile, str_pad((string)$file->getTimestamp(), self::TIMESTAMP_STR_LENGTH, "0"));
                fwrite($tmpFile, $file->getKey());
                fwrite($tmpFile, "\0");
                fwrite($tmpFile, base64_encode(serialize($file)));
                fwrite($tmpFile, "\n");
            }
        } finally {
            fclose($tmpFile);
        }

        return $tmpFilePath;
    }

    /**
     * @return \Iterator|File[]
     */
    private function listFiles(): \Iterator
    {
        $this->logger->info('Listing files to be downloaded');
        if (substr($this->key, -1) == '*') {
            return $this->listFilesInPrefix();
        } else {
            return $this->listSingleFile();
        }
    }

    /**
     * @return \Iterator|File[]
     */
    private function listFilesInPrefix(): \Iterator
    {
        $paginator = $this->client->getPaginator(
            'ListObjectsV2',
            [
                'Bucket' => $this->bucket,
                'Prefix' => substr($this->key, 0, -1),
                'MaxKeys' => self::MAX_OBJECTS_PER_PAGE
            ]
        );

        $filesListedCount = 0;
        $filesMatchedCount = 0;
        $newFilesCount = 0;
        /** @var array{Contents: ?array} $page */
        foreach ($paginator as $page) {
            $objects = $page['Contents'] ?? [];
            foreach ($objects as $object) {
                /** @var array{StorageClass: string, Key: string, Size: string, LastModified: \DateTimeInterface} $object */
                $filesListedCount++;
                if ($this->isFileIgnored($object)) {
                    continue;
                }

                $filesMatchedCount++;
                $dst = $this->getFileDestination($object['Key']);
                $file = new File($this->bucket, $object['Key'], $object['LastModified'], (int)$object['Size'], $dst);

                // log progress
                /** @phpstan-ignore-next-line */
                if ($filesListedCount !== 0 &&
                    $filesMatchedCount !== 0 &&
                    (($filesListedCount % 10000) === 0 || ($filesMatchedCount % 1000) === 0)
                ) {
                    $this->logger->info(sprintf(
                        'Listed %s files (%s matching the filter so far)',
                        $filesListedCount,
                        $filesMatchedCount
                    ));
                }

                if ($this->isFileOld($file)) {
                    continue;
                }

                $newFilesCount++;
                yield $file;
            }
        }
        $this->logger->info(sprintf('Found %s file(s)', $filesMatchedCount));

        if ($this->newFilesOnly) {
            $this->logger->info(sprintf('There are %s new file(s)', $newFilesCount));
        }

        if ($this->limit > 0 && $newFilesCount > $this->limit) {
            $this->logger->info("Downloading only {$this->limit} oldest file(s) out of " . $newFilesCount);
        }
    }

    /**
     * @return \Iterator|File[]
     */
    private function listSingleFile(): iterable
    {
        if ($this->includeSubFolders) {
            throw new UserException("Cannot include subfolders without wildcard.");
        }

        /** @var array{ContentLength: int, LastModified: \DateTimeInterface} $head */
        $head = $this->client->headObject([
            'Bucket' => $this->bucket,
            'Key' => $this->key,
        ])->toArray();

        $dst = $this->subFolder . basename($this->key);
        $file = new File($this->bucket, $this->key, $head['LastModified'], $head['ContentLength'], $dst);
        $this->logger->info('Found 1 file(s)');

        if ($this->isFileOld($file)) {
            $this->logger->info('There are 0 new file(s)');
            return;
        }

        yield $file;
        if ($this->newFilesOnly) {
            $this->logger->info('There are 1 new file(s)');
        }
    }

    private function getFileDestination(string $key): string
    {
        // remove wilcard mask from search key
        $keyWithoutWildcard = trim($this->key, "*");

        // search key contains folder
        $dirPrefixToBeRemoved = '';
        if (strrpos($keyWithoutWildcard, '/') !== false) {
            $dirPrefixToBeRemoved = substr($keyWithoutWildcard, 0, strrpos($keyWithoutWildcard, '/'));
        }

        // remove folder mask from object key to figure out, if there is a subfolder
        $objectKeyWithoutDirPrefix = substr($key, strlen($dirPrefixToBeRemoved));

        // trim object key without dir and figure out the dir name
        $dstDir = trim(dirname($objectKeyWithoutDirPrefix), '/');

        // complete path
        if ($this->includeSubFolders) {
            if ($dstDir && $dstDir != '.') {
                $flattened = str_replace(
                    '/',
                    '-',
                    str_replace('-', '--', $dstDir . '/' . basename($key))
                );
            } else {
                $flattened = str_replace(
                    '/',
                    '-',
                    str_replace('-', '--', basename($key))
                );
            }
            return $this->subFolder . $flattened;
        } else {
            return $this->subFolder . basename($key);
        }
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
        if (strrpos($object['Key'], '/', strlen($this->key) - 1) !== false && !$this->includeSubFolders) {
            return true;
        }

        // Skip empty folder files (https://github.com/keboola/aws-s3-extractor/issues/21)
        if (substr($object['Key'], -1, 1) === '/') {
            return true;
        }

        return false;
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
