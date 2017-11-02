<?php

namespace Keboola\S3Extractor;

use Monolog\Handler\HandlerInterface;
use Monolog\Logger;
use Symfony\Component\Config\Definition\Processor;

class Application
{
    /**
     * @var array
     */
    private $config;

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
     * Application constructor.
     *
     * @param $config
     * @param array $state
     * @param HandlerInterface|null $handler
     */
    public function __construct($config, array $state = [], HandlerInterface $handler = null)
    {
        $this->config = $config;
        $this->state = $state;
        $parameters = (new Processor)->processConfiguration(
            new ConfigDefinition,
            [$this->config['parameters']]
        );
        $this->parameters = $parameters;
        $logger = new Logger('Log');
        if ($handler) {
            $logger->pushHandler($handler);
        }
        $this->logger = $logger;
    }

    /**
     * Runs data extraction
     * @param $outputPath
     * @return bool
     * @throws \Exception
     */
    public function actionRun($outputPath)
    {
        $extractor = new Extractor($this->parameters, $this->state, $this->logger);
        return $extractor->extract($outputPath);
    }
}
