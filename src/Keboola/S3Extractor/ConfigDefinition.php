<?php

namespace Keboola\S3Extractor;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    public const LOGIN_TYPE_CREDENTIALS = 'credentials';

    public const LOGIN_TYPE_ROLE = 'role';

    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $this->addValidate($parametersNode);

        $parametersNode
            ->children()
                ->enumNode('loginType')
                    ->values([self::LOGIN_TYPE_CREDENTIALS, self::LOGIN_TYPE_ROLE])
                    ->defaultValue(self::LOGIN_TYPE_CREDENTIALS)
                ->end()
                ->scalarNode('accessKeyId')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('#secretAccessKey')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('accountId')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('roleName')
                    ->defaultValue('keboola-s3-extractor')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('externalId')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('bucket')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('key')
                    ->isRequired()
                    ->cannotBeEmpty()
                    ->validate()
                        ->ifTrue(static function ($key) {
                            return substr($key, -1) === '/';
                        })
                        ->thenInvalid('Use the wildcard flag or enter a full path to the file.')
                    ->end()
                ->end()
                ->booleanNode('includeSubfolders')
                    ->defaultFalse()
                ->end()
                ->booleanNode('newFilesOnly')
                    ->defaultFalse()
                ->end()
                ->scalarNode('saveAs')
                    ->defaultValue('')
                ->end()
                ->integerNode('limit')
                    ->defaultValue(0)
                    ->min(0)
                ->end()
            ->end()
        ;
       // @formatter:on
        return $parametersNode;
    }

    private function addValidate(NodeDefinition $definition): void
    {
        $definition->validate()->always(function ($item) {
            if ($item['loginType'] === self::LOGIN_TYPE_CREDENTIALS) {
                if (!isset($item['accessKeyId'])) {
                    throw new InvalidConfigurationException('The child node "accessKeyId" at path "root.parameters" must be configured.');
                }
                if (!isset($item['#secretAccessKey'])) {
                    throw new InvalidConfigurationException('The child node "#secretAccessKey" at path "root.parameters" must be configured.');
                }
            } elseif ($item['loginType'] === self::LOGIN_TYPE_ROLE) {
                if (!isset($item['accountId'])) {
                    throw new InvalidConfigurationException('The child node "accountId" at path "root.parameters" must be configured.');
                }
                if (!isset($item['roleName'])) {
                    throw new InvalidConfigurationException('The child node "roleName" at path "root.parameters" must be configured.');
                }
                if (!isset($item['externalId'])) {
                    throw new InvalidConfigurationException('The child node "externalId" at path "root.parameters" must be configured.');
                }
            } else {
                throw new InvalidConfigurationException(sprintf('Unrecognized login type "%s".', $item['loginType']));
            }
            return $item;
        })->end();
    }
}
