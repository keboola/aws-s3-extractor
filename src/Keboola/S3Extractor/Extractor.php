<?php
namespace Keboola\S3Extractor;

use Aws\Api\DateTimeResult;
use Aws\S3\S3Client;
use Monolog\Handler\NullHandler;
use Monolog\Logger;
use Symfony\Component\Filesystem\Filesystem;

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
        $region = $client->getBucketLocation(["Bucket" => $this->parameters["bucket"]])->get('LocationConstraint');
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
            $iterator = $client->getIterator('ListObjects', [
                'Bucket' => $this->parameters['bucket'],
                'Prefix' => substr($key, 0, -1)
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
                if ($this->parameters['includeSubfolders']) {
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
                    $dst = $outputPath . '/' . $this->parameters['saveAs'] . '/' . $flattened;
                } else {
                    $dst = $outputPath . '/' . $this->parameters['saveAs'] . '/' . basename($object['Key']);
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
            $dst = $outputPath . '/' . $this->parameters['saveAs'];
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

        $fs = new Filesystem();

        $downloadedFiles = 0;
        foreach ($filesToDownload as $fileToDownload) {
            // create folder

            if (!$fs->exists(dirname($fileToDownload['SaveAs']))) {
                $fs->mkdir(dirname($fileToDownload['SaveAs']));
            }
            $this->logger->info("Downloading file /" . $fileToDownload["Key"]);
            $client->getObject($fileToDownload);
            $downloadedFiles++;
        }
        $this->logger->info("Downloaded {$downloadedFiles} file(s)");
        return $nextState;
    }
}
