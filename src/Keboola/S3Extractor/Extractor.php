<?php
namespace Keboola\S3Extractor;

use Aws\Api\DateTimeResult;
use Aws\S3\S3Client;
use Aws\S3\S3MultiRegionClient;
use Monolog\Handler\NullHandler;
use Monolog\Logger;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\Component\UserException;

class Extractor
{
    /**
     * @var Config
     */
    private $config;

    /**
     * @var Logger
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
    public function extract($outputPath)
    {
        $client = new S3MultiRegionClient([
            'version' => '2006-03-01',
            'credentials' => [
                'key' => $this->config->accessKeyId(),
                'secret' => $this->config->secretAccessKey(),
            ],
        ]);
        $region = $client->getBucketLocation(["Bucket" => $this->config->bucket()])->get('LocationConstraint');
        $client = new S3Client([
            'region' => $region,
            'version' => '2006-03-01',
            'credentials' => [
                'key' => $this->config->accessKeyId(),
                'secret' => $this->config->secretAccessKey(),
            ],
        ]);

        // Remove initial forwardslash
        $key = $this->config->key();
        if (substr($key, 0, 1) == '/') {
            $key = substr($key, 1);
        }

        $saveAsSubfolder = '';
        if (!empty($this->config->saveAs())) {
            $saveAsSubfolder = $this->config->saveAs(). '/';
        }

        $filesToDownload = [];

        // Detect wildcard at the end
        if (substr($key, -1) == '*') {
            $iterator = $client->getIterator('ListObjects', [
                'Bucket' => $this->config->bucket(),
                'Prefix' => substr($key, 0, -1),
            ]);
            foreach ($iterator as $object) {
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
                if (strrpos($object['Key'], '/', strlen($key) - 1) !== false && !$this->config->isUncludeSubfolders()) {
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
                if ($this->config->isUncludeSubfolders()) {
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
                    'Bucket' => $this->config->bucket(),
                    'Key' => $object['Key'],
                    'SaveAs' => $dst,
                ];
                $filesToDownload[] = [
                    "timestamp" => $object['LastModified']->format("U"),
                    "parameters" => $parameters,
                ];
            }
        } else {
            if ($this->config->isUncludeSubfolders() === true) {
                throw new UserException("Cannot include subfolders without wildcard.");
            }
            $dst = $outputPath . '/' . $saveAsSubfolder . basename($key);
            $parameters = [
                'Bucket' => $this->config->bucket(),
                'Key' => $key,
                'SaveAs' => $dst,
            ];
            $head = $client->headObject($parameters);
            $filesToDownload[] = [
                "timestamp" => $head["LastModified"]->format("U"),
                "parameters" => $parameters,
            ];
        }

        // Timestamp of last downloaded file, processed files in the last timestamp second
        $lastDownloadedFileTimestamp = isset($this->state['lastDownloadedFileTimestamp']) ? $this->state['lastDownloadedFileTimestamp'] : 0;
        $processedFilesInLastTimestampSecond = isset($this->state['processedFilesInLastTimestampSecond']) ? $this->state['processedFilesInLastTimestampSecond'] : [];

        // Filter out old files with newFilesOnly flag
        if ($this->config->isNewFilesOnly() === true) {
            $filesToDownload = array_filter($filesToDownload, function ($fileToDownload) use (
                $lastDownloadedFileTimestamp,
                $processedFilesInLastTimestampSecond
            ) {
                /** @var DateTimeResult $lastModified */
                if ($fileToDownload["timestamp"] < $lastDownloadedFileTimestamp) {
                    return false;
                }
                if (in_array($fileToDownload["parameters"]["Key"], $processedFilesInLastTimestampSecond)) {
                    return false;
                }
                return true;
            });
        }

        // Apply limit if set
        if ($this->config->limit() > 0 && count($filesToDownload) > $this->config->limit()) {
            // Sort files to download using timestamp
            usort($filesToDownload, function ($a, $b) {
                if (intval($a["timestamp"]) - intval($b["timestamp"]) === 0) {
                    return strcmp($a["parameters"]["Key"], $b["parameters"]["Key"]);
                }
                return intval($a["timestamp"]) - intval($b["timestamp"]);
            });
            $this->logger->info("Downloading only {$this->config->limit()} oldest file(s) out of " . count($filesToDownload));
            $filesToDownload = array_slice($filesToDownload, 0, $this->config->limit());
        }

        $fs = new Filesystem();
        $downloadedFiles = 0;

        // Download files
        foreach ($filesToDownload as $fileToDownload) {
            // create folder
            if (!$fs->exists(dirname($fileToDownload["parameters"]['SaveAs']))) {
                $fs->mkdir(dirname($fileToDownload["parameters"]['SaveAs']));
            }
            $this->logger->info("Downloading file /" . $fileToDownload["parameters"]["Key"]);

            DownloadFile::process($client, $this->logger, $fileToDownload['parameters']);

            if ($lastDownloadedFileTimestamp != $fileToDownload["timestamp"]) {
                $processedFilesInLastTimestampSecond = [];
            }
            $lastDownloadedFileTimestamp = max($lastDownloadedFileTimestamp, $fileToDownload["timestamp"]);
            $processedFilesInLastTimestampSecond[] = $fileToDownload["parameters"]["Key"];
            $downloadedFiles++;
        }
        $this->logger->info("Downloaded {$downloadedFiles} file(s)");

        if ($this->config->isNewFilesOnly() === true) {
            return [
                'lastDownloadedFileTimestamp' => $lastDownloadedFileTimestamp,
                'processedFilesInLastTimestampSecond' => $processedFilesInLastTimestampSecond,
            ];
        } else {
            return [];
        }
    }
}
