<?php

namespace Keboola\S3Extractor;

use Aws\S3\S3Client;
use Psr\Log\LoggerInterface;

/**
 * Finder search for file for download.
 */
class Finder {
    private const MAX_OBJECTS_PER_PAGE = 100;

    /** @var LoggerInterface */
    private $logger;

    /** @var S3Client */
    private $client;

    /** @var string */
    private $key;

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
        $this->key = $config->getKey();
        if (!empty($config->getSaveAs())) {
            $this->subFolder = $config->getSaveAs() . '/';
        }
        $this->limit = $config->getLimit();
        $this->state = $state;
    }
}
