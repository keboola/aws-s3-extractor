<?php
namespace Keboola\S3Extractor;

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
     * Extractor constructor.
     *
     * @param array $parameters
     * @param Logger|null $logger
     */
    public function __construct(array $parameters, Logger $logger = null)
    {
        $this->parameters = $parameters;
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
     * @return bool
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
        if (substr($key, -1) == '*' || substr($key, - 1) == '%') {
            try {
                $iterator = $client->getIterator('ListObjects', [
                    'Bucket' => $this->parameters['bucket'],
                    'Prefix' => substr($key, 0, strlen($key) - 1)
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

                // Skip objects in subfolders
                if (strrpos($object['Key'], '/', strlen($key)) !== false) {
                    continue;
                }
                $dst = $outputPath . '/' . substr($object['Key'], strrpos($object['Key'], '/'));
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

        $downloadedFiles = 0;
        foreach ($filesToDownload as $fileToDownload) {
            try {
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
    }
}
