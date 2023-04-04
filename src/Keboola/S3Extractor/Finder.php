<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Keboola\Component\UserException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;

class Finder
{
    private const MAX_OBJECTS_PER_PAGE = 1000;
    private const TIMESTAMP_STR_LENGTH = 12;

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

    /** @var int */
    private $limit;

    /**
     * Count of all files returned by the API.
     * @var int
     */
    private $listedCount = 0;

    /**
     * Count of all not ignored files (see isFileIgnored method).
     * @var int
     */
    private $matchedCount = 0;

    /**
     * Count of all new files (see isFileOld method).
     * If newFilesOnly=false, then the value is equal to $matchedCount.
     * @var int
     */
    private $newCount = 0;

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
        $this->limit = $this->config->getLimit();
    }

    public function listFiles(): FinderResult
    {
        $this->logger->info('Listing files to be downloaded');
        $tmpFilePath = $this->listFilesToTmpFile();
        $this->logger->info(sprintf('Found %s file(s)', $this->matchedCount));
        if ($this->config->isNewFilesOnly()) {
            $this->logger->info(sprintf('There are %s new file(s)', $this->newCount));
        }

        // Sort by timestamp
        $sortedFilePath = $this->sortTmpFile($tmpFilePath);

        // Compute total count and size, take into account the limit
        $count = 0;
        $size = 0;
        $state = clone $this->oldState;
        foreach ($this->iteratorFromTmpFile($sortedFilePath, false) as $file) {
            $count++;
            $size += $file->getSizeBytes();
            $state->lastTimestamp = max($state->lastTimestamp, $file->getTimestamp());
            if ($state->lastTimestamp != $file->getTimestamp()) {
                $state->filesInLastTimestamp = [];
            } else {
                $state->filesInLastTimestamp[] = $file->getKey();
            }
        }
        if ($this->limit > 0 && $this->newCount > $this->limit) {
            $this->logger->info("Downloading only {$count} oldest file(s) out of " . $this->newCount);
        }

        // Rewind the sorted file and return the iterator
        $iterator = $this->iteratorFromTmpFile($sortedFilePath, true);
        return new FinderResult($iterator, $count, $size, $state);
    }

    /**
     * @return S3File[]
     */
    private function iteratorFromTmpFile(string $sortedFilePath, bool $unlink): iterable
    {
        // Read sorted metadata
        $sortedFile = fopen($sortedFilePath, "r");
        if (!$sortedFile) {
            throw new \RuntimeException(sprintf('Cannot open sorted file "%s".', $sortedFilePath));
        }
        try {
            $i = 0;
            while (($line = fgets($sortedFile, 20480)) !== false) {
                // Skip timestamp + space character
                $serialized = substr($line, self::TIMESTAMP_STR_LENGTH + 1);
                /** @var S3File $file */
                $file = unserialize(base64_decode($serialized));
                yield $file;
                if ($this->limit > 0 && ++$i >= $this->limit) {
                    break;
                }
            }
        } finally {
            fclose($sortedFile);
            if ($unlink) {
                unlink($sortedFilePath);
            }
        }
    }

    private function sortTmpFile(string $tmpFilePath): string
    {
        // Sort metadata by timestamp using "sort" command
        $sortedFilePath = $tmpFilePath . ".sorted";
        (new Process(["sort", "-k1.1,1." . self::TIMESTAMP_STR_LENGTH, "--parallel=1", "--output", $sortedFilePath, $tmpFilePath]))->mustRun();
        unlink($tmpFilePath);
        return $sortedFilePath;
    }

    private function listFilesToTmpFile(): string
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
            foreach ($this->listFilesIterator() as $file) {
                // Write each file metadata as: <timestamp> <serialized S3File object>\n
                // The base64 encoding is used to prevent new lines in the serialized object.
                fwrite($tmpFile, str_pad((string)$file->getTimestamp(), self::TIMESTAMP_STR_LENGTH, "0"));
                fwrite($tmpFile, " ");
                fwrite($tmpFile, base64_encode(serialize($file)));
                fwrite($tmpFile, "\n");
            }
        } finally {
            fclose($tmpFile);
        }

        return $tmpFilePath;
    }

    /**
     * @return S3File[]
     */
    private function listFilesIterator(): iterable
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
        $keyWithoutWildcard = rtrim($this->key, "*");

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
                $this->newCount++;

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
            if ($file->getTimestamp() < $this->oldState->lastTimestamp) {
                return true;
            }

            if ($file->getTimestamp() === $this->oldState->lastTimestamp
                && in_array($file->getKey(), $this->oldState->filesInLastTimestamp)
            ) {
                return true;
            }
        }

        return false;
    }
}
