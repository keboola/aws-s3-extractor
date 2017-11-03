<?php
namespace Keboola\S3Extractor;

use Aws\Api\DateTimeResult;
use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Monolog\Handler\NullHandler;
use Monolog\Logger;

class Extractor
{
    /**
     * @var array
     */
    private $parameters;

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
     * @param array $parameters
     * @param array $state
     * @param Logger|null $logger
     */
    public function __construct(array $parameters, array $state = [], Logger $logger = null)
    {
        $this->parameters = $parameters;
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
     * @param $outputPath
     * @return array
     * @throws \Exception
     */
    public function extract($outputPath)
    {
        $client = new S3Client([
            'region' => 'us-east-1',
            'version' => '2006-03-01',
            'credentials' => [
                'key' => $this->parameters['accessKeyId'],
                'secret' => $this->parameters['#secretAccessKey'],
            ]
        ]);
        try {
            $region = $client->getBucketLocation(["Bucket" => $this->parameters["bucket"]])->get('LocationConstraint');
        } catch (S3Exception $e) {
            if ($e->getStatusCode() == 404) {
                throw new Exception("Bucket {$this->parameters["bucket"]} not found.");
            }
            if ($e->getStatusCode() == 403) {
                throw new Exception("Invalid credentials or permissions not set correctly. Did you set s3:GetBucketLocation?");
            }
            throw $e;
        }
        $client = new S3Client([
            'region' => $region,
            'version' => '2006-03-01',
            'credentials' => [
                'key' => $this->parameters['accessKeyId'],
                'secret' => $this->parameters['#secretAccessKey'],
            ]
        ]);

        // Remove initial forwardslash
        $key = $this->parameters['key'];
        if (substr($key, 0, 1) == '/') {
            $key = substr($key, 1);
        }

        $filesToDownload = [];

        // Detect wildcard at the end
        if (substr($key, -1) == '*') {
            try {
                $iterator = $client->getIterator('ListObjects', [
                    'Bucket' => $this->parameters['bucket'],
                    'Prefix' => substr($key, 0, -1)
                ]);
            } catch (S3Exception $e) {
                if ($e->getStatusCode() == 403) {
                    throw new Exception("Invalid credentials or permissions not set correctly. Did you set s3:ListObjects?");
                }
                throw $e;
            }

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
                if (strrpos($object['Key'], '/', strlen($key)) !== false && !$this->parameters['includeSubfolders']) {
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
                if ($dstDir) {
                    $dst = $outputPath . '/' . $dstDir . '/' . basename($object['Key']);
                } else {
                    $dst = $outputPath . '/' . basename($object['Key']);
                }

                $filesToDownload[] = [
                    'Bucket' => $this->parameters['bucket'],
                    'Key' => $object['Key'],
                    'SaveAs' => $dst
                ];
            }
        } else {
            if ($this->parameters['includeSubfolders'] === true) {
                throw new Exception("Cannot include subfolders without wildcard.");
            }
            $dst = $outputPath . '/' . substr($key, strrpos($key, '/'));
            $filesToDownload[] = [
                'Bucket' => $this->parameters['bucket'],
                'Key' => $key,
                'SaveAs' => $dst
            ];
        }

        // Filter out old files with newFilesOnly flag
        if ($this->parameters['newFilesOnly'] === true) {
            $lastDownloadedFileTimestamp = isset($this->state['lastDownloadedFileTimestamp']) ? $this->state['lastDownloadedFileTimestamp'] : 0;
            $newLastDownloadedFileTimestamp = $lastDownloadedFileTimestamp;
            $filesToDownload = array_filter($filesToDownload, function ($fileToDownload) use ($client, $lastDownloadedFileTimestamp, &$newLastDownloadedFileTimestamp) {
                $object = $client->headObject($fileToDownload);
                /** @var DateTimeResult $lastModified */
                $lastModified = $object["LastModified"];
                if ($lastModified->format("U") > $lastDownloadedFileTimestamp) {
                    $newLastDownloadedFileTimestamp = max($newLastDownloadedFileTimestamp, $lastModified->format("U"));
                    return true;
                }
                return false;
            });
            $nextState['lastDownloadedFileTimestamp'] = $newLastDownloadedFileTimestamp;
        } else {
            $nextState = [];
        }

        $downloadedFiles = 0;
        foreach ($filesToDownload as $fileToDownload) {
            try {
                // create folder
                if (!file_exists(dirname($fileToDownload['SaveAs']))) {
                    mkdir(dirname($fileToDownload['SaveAs']), 0777, true);
                }
                $this->logger->info("Downloading file /" . $fileToDownload["Key"]);
                $client->getObject($fileToDownload);
                $downloadedFiles++;
            } catch (S3Exception $e) {
                if ($e->getStatusCode() == 404) {
                    throw new Exception("File {$fileToDownload["Key"]} not found.");
                }
                if ($e->getStatusCode() == 403) {
                    throw new Exception("Invalid credentials or permissions not set correctly. Did you set s3:GetObject?");
                }
                throw $e;
            }
        }
        $this->logger->info("Downloaded {$downloadedFiles} file(s)");
        return $nextState;
    }
}
