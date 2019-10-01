<?php

namespace Keboola\S3Extractor;

use Aws\S3\Exception\S3Exception;
use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;

class Application extends BaseComponent
{
    /**
     * @throws UserException
     */
    protected function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();

        $extractor = new Extractor($config, $this->getInputState(), $this->getLogger());
        try {
            $this->writeOutputStateToFile(
                $extractor->extract($this->getOutputDirectory())
            );
        } catch (S3Exception $e) {
            S3ExceptionConverter::resolve($e, $config->getKey());
        }
    }

    /**
     * @return string
     */
    private function getOutputDirectory(): string
    {
        return $this->getDataDir() . '/out/files/';
    }

    /**
     * @return string
     */
    protected function getConfigClass(): string
    {
        return Config::class;
    }

    /**
     * @return string
     */
    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
