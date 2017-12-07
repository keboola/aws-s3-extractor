<?php

namespace Keboola\S3Extractor;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ConfigDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

        $rootNode
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
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->integerNode('limit')
                    ->isRequired()
                    ->cannotBeEmpty()
                    ->defaultValue(1000)
                    ->min(1)
                    ->max(1000)
                ->end()

            ->end()
        ;

        return $treeBuilder;
    }
}
