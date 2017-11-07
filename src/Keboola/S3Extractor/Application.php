<?php

namespace Keboola\S3Extractor;

use Aws\S3\Exception\S3Exception;
use GuzzleHttp\Exception\ClientException;
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
     * @return array
     * @throws \Exception
     */
    public function actionRun($outputPath)
    {
        $extractor = new Extractor($this->parameters, $this->state, $this->logger);
        try {
            return $extractor->extract($outputPath);
        } catch (S3Exception $e) {
            if ($e->getStatusCode() === 403) {
                throw new Exception("Invalid credentials or permissions.", $e->getCode(), $e);
            }
            if ($e->getStatusCode() === 400 || $e->getStatusCode() === 401 || $e->getStatusCode() === 404) {
                if (get_class($e->getPrevious()) === ClientException::class) {
                    /** @var ClientException $previous */
                    $previous = $e->getPrevious();
                    throw new Exception(
                        $previous->getResponse()->getStatusCode()
                        . " "
                        . $previous->getResponse()->getReasonPhrase()
                        . " ("
                        . $e->getAwsErrorCode()
                        . ")\n"
                        . $previous->getResponse()->getBody()->__toString()
                    );
                }
                throw new Exception($e->getMessage());
            }
            throw $e;
        }
    }
}
