<?php

namespace Keboola\S3Extractor;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    /**
     * @return ArrayNodeDefinition
     */
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->scalarNode('accessKeyId')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('#secretAccessKey')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('bucket')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('key')
                    ->isRequired()
                    ->cannotBeEmpty()
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
}
