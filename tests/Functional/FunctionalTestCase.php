<?php

declare(strict_types=1);

namespace Keboola\S3ExtractorTest\Functional;

use Aws\S3\S3Client;
use Keboola\Component\JsonHelper;
use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;

class FunctionalTestCase extends AbstractDatadirTestCase
{
    protected const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    protected const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    protected const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';
    protected const UPDATE_AWS_S3_ACCESS_KEY_ENV = 'UPLOAD_USER_AWS_ACCESS_KEY';
    protected const UPDATE_AWS_S3_SECRET_KEY_ENV = 'UPLOAD_USER_AWS_SECRET_KEY';
    protected const UPDATE_AWS_S3_BUCKET = 'AWS_S3_BUCKET';
    protected const UPDATE_AWS_REGION = 'AWS_REGION';

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        (new Process('php ' . __DIR__ . '/../loadS3.php'))
            ->setTimeout(1000)
            ->mustRun();
    }

    /**
     * @return S3Client
     */
    protected static function s3Client(): S3Client
    {
        return new S3Client([
            'region' => getenv(self::UPDATE_AWS_REGION),
            'version' => '2006-03-01',
            'credentials' => [
                'key' => getenv(self::UPDATE_AWS_S3_ACCESS_KEY_ENV),
                'secret' => getenv(self::UPDATE_AWS_S3_SECRET_KEY_ENV),
            ],
        ]);
    }

    /**
     * @param string $key
     * @return string
     */
    protected static function getS3FileLastModified(string $key): string
    {
        $headObject = self::s3Client()->headObject([
            'Bucket' => getenv(self::AWS_S3_BUCKET_ENV),
            'Key' => $key,
        ]);

        return $headObject['LastModified']->format('U');
    }

    /**
     * @param string $testDirectory
     * @param array $processedFiles
     * @param string|null $forceTimestamp
     * @throws JsonHelper\JsonHelperException
     */
    protected static function writeOutStateFile(
        string $testDirectory,
        array $processedFiles,
        string $forceTimestamp = null
    ): void {
        self::writeStateFile(
            sprintf('%s/expected/data/out/state.json', $testDirectory),
            $processedFiles,
            $forceTimestamp
        );
    }

    /**
     * @param string $testDirectory
     * @param array $processedFiles
     * @param string|null $forceTimestamp
     * @throws JsonHelper\JsonHelperException
     */
    protected static function writeInStateFile(
        string $testDirectory,
        array $processedFiles,
        string $forceTimestamp = null
    ): void {
        self::writeStateFile(
            sprintf('%s/source/data/in/state.json', $testDirectory),
            $processedFiles,
            $forceTimestamp
        );
    }

    /**
     * @param array $messages
     * @return string
     */
    protected static function convertToStdout(array $messages): string
    {
        return implode(PHP_EOL, $messages) . PHP_EOL;
    }

    /**
     * @param Temp $temp
     * @param array $config
     * @throws JsonHelper\JsonHelperException
     */
    protected static function writeConfigFile(Temp $temp, array $config): void
    {
        JsonHelper::writeFile($temp->getTmpFolder() . '/config.json', $config);
    }

    /**
     * @param string $fileName
     * @param array $processedFiles
     * @param string|null $forceTimestamp
     * @throws JsonHelper\JsonHelperException
     */
    private static function writeStateFile(
        string $fileName,
        array $processedFiles,
        string $forceTimestamp = null
    ): void {
        JsonHelper::writeFile($fileName, [
            'lastDownloadedFileTimestamp' => $forceTimestamp ?: self::getS3FileLastModified($processedFiles[0]),
            'processedFilesInLastTimestampSecond' => $processedFiles,
        ]);
    }
}
