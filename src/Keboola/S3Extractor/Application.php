<?php

namespace Keboola\S3Extractor;

use Aws\S3\Exception\S3Exception;
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
     * @param array $config
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
     * @param string $outputPath
     * @return array
     * @throws Exception
     */
    public function actionRun($outputPath)
    {
        if (substr($this->parameters['key'], -1) === '/') {
            throw new Exception('Use the wildcard flag or enter a full path to the file.');
        }

        $extractor = new Extractor($this->parameters, $this->state, $this->logger);
        try {
            return $extractor->extract($outputPath);
        } catch (S3Exception $e) {
            S3ExceptionConverter::resolve($e, $this->parameters['key']);
        }
    }
}
