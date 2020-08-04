<?php

namespace Keboola\S3Extractor;

use Aws\S3\Exception\S3Exception;
use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;

class Application extends BaseComponent
{
    private const ACTION_GET_EXTERNAL_ID = 'getExternalId';

    private const ACTION_RUN = 'run';

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

    public function getExternalIdAction(): array
    {
        /** @var Config $config */
        $config = $this->getConfig();

        $extractor = new Extractor($config, $this->getInputState(), $this->getLogger());

        return ['external-id' => $extractor->getExternalId()];
    }

    protected function getSyncActions(): array
    {
        return [
            self::ACTION_GET_EXTERNAL_ID => 'getExternalIdAction',
        ];
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
        $action = $this->getRawConfig()['action'] ?? self::ACTION_RUN;
        switch ($action) {
            case self::ACTION_RUN:
                return ConfigDefinition::class;
            case self::ACTION_GET_EXTERNAL_ID:
                return GetExternalIdDefinition::class;
            default:
                throw new UserException(sprintf('Unexpected action "%s"', $action));
        }
    }
}
