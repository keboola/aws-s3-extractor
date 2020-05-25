<?php

namespace Keboola\S3Extractor;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $this->addValidate($parametersNode);

        $parametersNode
            ->children()
                ->scalarNode('loginType')
                    ->cannotBeEmpty()
                    ->defaultValue('credentials')
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
            if (!isset($item['loginType']) || $item['loginType'] === 'credentials') {
                if (!isset($item['accessKeyId'])) {
                    throw new InvalidConfigurationException('The child node "accessKeyId" at path "root.parameters" must be configured.');
                }
                if (!isset($item['#secretAccessKey'])) {
                    throw new InvalidConfigurationException('The child node "#secretAccessKey" at path "root.parameters" must be configured.');
                }
            } elseif ($item['loginType'] === 'role') {
                if (!isset($item['accountId'])) {
                    throw new InvalidConfigurationException('The child node "accountId" at path "root.parameters" must be configured.');
                }
                if (!isset($item['roleName'])) {
                    throw new InvalidConfigurationException('The child node "roleName" at path "root.parameters" must be configured.');
                }
            }
            return $item;
        })->end();
    }
}
