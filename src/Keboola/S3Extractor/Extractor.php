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
    const MAX_OBJECTS_PER_PAGE = 100;
    /**
     * @var Config
     */
    private $config;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var State
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
        $this->state = new State($state);
        if ($logger) {
            $this->logger = $logger;
        } else {
            $this->logger = new Logger('dummy');
            $this->logger->pushHandler(new NullHandler());
        }
    }

    /**
     * Creates exports and runs extraction
     */
    public function extract(string $outputDir): array
    {
        $client = $this->login();

        $finder = new Finder($this->config, $this->state, $this->logger, $client);
        $foundFiles = $finder->findFiles();

        $fs = new Filesystem();
        $downloader = new S3AsyncDownloader($client, $fs, $this->logger);

        // Download files
        $downloadedSize = 0;
        foreach ($foundFiles->getIterator() as $fileToDownload) {
            $downloader->addFileRequest($fileToDownload->getParameters($outputDir));

            if ($this->state->lastTimestamp != $fileToDownload->getTimestamp()) {
                $this->state->filesInLastTimestamp = [];
            }
            $this->state->lastTimestamp = max($this->state->lastTimestamp, $fileToDownload->getTimestamp());
            $this->state->filesInLastTimestamp[] = $fileToDownload->getKey();
        }

        if ($foundFiles->getCount() > 0) {
            $this->logger->info(sprintf(
                'Downloading %d file(s) (%s)',
                $foundFiles->getCount(),
                formatBytes($foundFiles->getDownloadSizeBytes())
            ));
        }

        $downloader->processRequests();

        if ($this->config->isNewFilesOnly() === true) {
            return $this->state->toArray();
        } else {
            return [];
        }
    }

    public function getExternalId(): string
    {
        return sprintf('%s-%s', getenv('KBC_STACKID'), getenv('KBC_PROJECTID'));
    }

    private function login(): S3Client
    {
        if ($this->config->getLoginType() === ConfigDefinition::LOGIN_TYPE_ROLE) {
            return $this->loginViaRole();
        }
        return $this->loginViaCredentials();
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
