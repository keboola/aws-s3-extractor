<?php

namespace Keboola\S3Extractor;

use Aws\Api\DateTimeResult;
use Aws\Credentials\Credentials;
use Aws\S3\S3Client;
use Aws\S3\S3MultiRegionClient;
use Aws\Sts\Exception\StsException;
use Aws\Sts\StsClient;
use DateTimeInterface;
use Monolog\Handler\NullHandler;
use Monolog\Logger;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\Component\UserException;
use function Keboola\Utils\formatBytes;

class Extractor
{
    const MAX_OBJECTS_PER_PAGE = 1000;
    /**
     * @var Config
     */
    private $config;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var array
     */
    private $state;

    /**
     * Extractor constructor.
     *
     * @param Config $config
     * @param array $state
     * @param LoggerInterface|null $logger
     */
    public function __construct(Config $config, array $state = [], LoggerInterface $logger = null)
    {
        $this->config = $config;
        $this->state = $state;
        if ($logger) {
            $this->logger = $logger;
        } else {
            $this->logger = new Logger('dummy');
            $this->logger->pushHandler(new NullHandler());
        }
    }

    /**
     * Creates exports and runs extraction
     *
     * @param string $outputPath
     * @return array
     * @throws UserException
     */
    public function extract($outputPath): array
    {
        if ($this->config->getLoginType() === ConfigDefinition::LOGIN_TYPE_ROLE) {
            $client = $this->loginViaRole();
        } else {
            $client = $this->loginViaCredentials();
        }

        // Remove initial forwardslash
        $key = $this->config->getKey();
        if (substr($key, 0, 1) == '/') {
            $key = substr($key, 1);
        }

        $saveAsSubfolder = '';
        if (!empty($this->config->getSaveAs())) {
            $saveAsSubfolder = $this->config->getSaveAs() . '/';
        }

        /** @var array<int, array{
         *     'timestamp': string,
         *     'size': int,
         *     'parameters': array{'Bucket': string, 'Key': string, 'SaveAs': string},
         * }> $filesToDownload
         */
        $filesToDownload = [];

        $this->logger->info('Listing files to be downloaded');

        // Detect wildcard at the end
        if (substr($key, -1) == '*') {
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
                     *     LastModified: DateTimeInterface,
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
                        $dst = $outputPath . '/' . $saveAsSubfolder . $flattened;
                    } else {
                        $dst = $outputPath . '/' . $saveAsSubfolder . basename($object['Key']);
                    }

                    $parameters = [
                        'Bucket' => $this->config->getBucket(),
                        'Key' => $object['Key'],
                        'SaveAs' => $dst,
                    ];
                    $filesToDownload[] = [
                        "timestamp" => $object['LastModified']->format("U"),
                        "size" => (int) $object['Size'],
                        "parameters" => $parameters,
                    ];
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
        } else {
            if ($this->config->isIncludeSubfolders() === true) {
                throw new UserException("Cannot include subfolders without wildcard.");
            }
            $dst = $outputPath . '/' . $saveAsSubfolder . basename($key);
            $parameters = [
                'Bucket' => $this->config->getBucket(),
                'Key' => $key,
                'SaveAs' => $dst,
            ];
            $head = $client->headObject($parameters);
            /** @var \Datetime $lastModified */
            $lastModified = $head['LastModified'];
            $filesToDownload[] = [
                "timestamp" => $lastModified->format("U"),
                "size" => $head->get('ContentLength'),
                "parameters" => $parameters,
            ];
        }

        // Timestamp of last downloaded file, processed files in the last timestamp second
        $lastDownloadedFileTimestamp = isset($this->state['lastDownloadedFileTimestamp']) ? $this->state['lastDownloadedFileTimestamp'] : 0;
        $processedFilesInLastTimestampSecond = isset($this->state['processedFilesInLastTimestampSecond']) ? $this->state['processedFilesInLastTimestampSecond'] : [];

        $this->logger->info(sprintf(
            'Found %s file(s)',
            count($filesToDownload)
        ));

        // Filter out old files with newFilesOnly flag
        if ($this->config->isNewFilesOnly() === true) {
            $filesToDownload = array_filter($filesToDownload, function ($fileToDownload) use (
                $lastDownloadedFileTimestamp,
                $processedFilesInLastTimestampSecond
            ) {
                if ($fileToDownload["timestamp"] < $lastDownloadedFileTimestamp) {
                    return false;
                }
                if ($fileToDownload["timestamp"] === $lastDownloadedFileTimestamp
                    && in_array($fileToDownload["parameters"]["Key"], $processedFilesInLastTimestampSecond)
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
        usort($filesToDownload, function ($a, $b) {
            if (intval($a["timestamp"]) - intval($b["timestamp"]) === 0) {
                return strcmp($a["parameters"]["Key"], $b["parameters"]["Key"]);
            }
            return intval($a["timestamp"]) - intval($b["timestamp"]);
        });

        // Apply limit if set
        if ($this->config->getLimit() > 0 && count($filesToDownload) > $this->config->getLimit()) {
            $this->logger->info("Downloading only {$this->config->getLimit()} oldest file(s) out of " . count($filesToDownload));
            $filesToDownload = array_slice($filesToDownload, 0, $this->config->getLimit());
        }

        $fs = new Filesystem();
        $downloader = new S3AsyncDownloader($client, $this->logger);

        // Download files
        $downloadedSize = 0;
        foreach ($filesToDownload as $fileToDownload) {
            // create folder
            if (!$fs->exists(dirname($fileToDownload["parameters"]['SaveAs']))) {
                $fs->mkdir(dirname($fileToDownload["parameters"]['SaveAs']));
            }

            $downloader->addFileRequest($fileToDownload['parameters']);

            if ($lastDownloadedFileTimestamp != $fileToDownload["timestamp"]) {
                $processedFilesInLastTimestampSecond = [];
            }
            $lastDownloadedFileTimestamp = max($lastDownloadedFileTimestamp, $fileToDownload["timestamp"]);
            $processedFilesInLastTimestampSecond[] = $fileToDownload["parameters"]["Key"];
            $downloadedSize += $fileToDownload['size'];
        }

        if (count($filesToDownload) > 0) {
            $this->logger->info(sprintf(
                'Downloading %d file(s) (%s)',
                count($filesToDownload),
                formatBytes($downloadedSize)
            ));
        }

        $downloader->processRequests();

        if ($this->config->isNewFilesOnly() === true) {
            return [
                'lastDownloadedFileTimestamp' => $lastDownloadedFileTimestamp,
                'processedFilesInLastTimestampSecond' => $processedFilesInLastTimestampSecond,
            ];
        } else {
            return [];
        }
    }

    public function getExternalId(): string
    {
        return sprintf('%s-%s', getenv('KBC_STACKID'), getenv('KBC_PROJECTID'));
    }

    private function loginViaCredentials(): S3Client
    {
        $awsCred = new Credentials($this->config->getAccessKeyId(), $this->config->getSecretAccessKey());
        return new S3Client([
            'region' => $this->getBucketRegion($awsCred),
            'version' => '2006-03-01',
            'credentials' => $awsCred,
            'retries' => 10
        ]);
    }

    private function loginViaRole(): S3Client
    {
        $awsCred = new Credentials($this->config->getKeboolaUserAwsAccessKey(), $this->config->getKeboolaUserAwsSecretKey());

        try {
            $stsClient = new StsClient([
                'region' => 'us-east-1',
                'version' => '2011-06-15',
                'credentials' => $awsCred,
            ]);

            $roleArn = sprintf('arn:aws:iam::%s:role/%s', $this->config->getAccountId(), $this->config->getRoleName());
            $result = $stsClient->assumeRole([
                'RoleArn' => $roleArn,
                'RoleSessionName' => 'KeboolaS3Extractor',
                'ExternalId' => $this->getExternalId(),
            ]);
        } catch (StsException $exception) {
            throw new UserException($exception->getMessage(), 0, $exception);
        }

        /** @var array $credentials */
        $credentials = $result->offsetGet('Credentials');
        $awsCred = new Credentials(
            (string) $credentials['AccessKeyId'],
            (string) $credentials['SecretAccessKey'],
            (string) $credentials['SessionToken']
        );

        return new S3Client([
            'region' => $this->getBucketRegion($awsCred),
            'version' => '2006-03-01',
            'credentials' => $awsCred,
        ]);
    }

    private function getBucketRegion(Credentials $credentials): string
    {
        $client = new S3MultiRegionClient([
            'version' => '2006-03-01',
            'credentials' => $credentials,
        ]);
        return $client->determineBucketRegion($this->config->getBucket()) ?: 'us-east-1';
    }
}
