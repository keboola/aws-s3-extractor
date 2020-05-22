<?php

namespace Keboola\S3ExtractorTest\Unit;

use Keboola\S3Extractor\Config;
use Keboola\S3Extractor\ConfigDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinitionTest extends TestCase
{
    public function testValidConfig()
    {
        $config = [
            'parameters' => [
                'accessKeyId' => 'a',
                '#secretAccessKey' => 'b',
                'bucket' => 'c',
                'key' => 'd',
                'includeSubfolders' => false,
                'newFilesOnly' => false,
                'saveAs' => 'myfile.csv',
                'limit' => 1,
                'loginType' => 'credentials',
            ],
        ];

        $this->assertSame(
            $config,
            (new Config($config, new ConfigDefinition))->getData()
        );
    }

    public function testInvalidConfig()
    {
        $config = [
            'parameters' => [
                'accessKeyId' => 'a',
                '#secretAccessKey' => 'b',
                'bucket' => 'c',
            ],
        ];

        $this->expectException(InvalidConfigurationException::class);

        new Config($config, new ConfigDefinition);
    }

    public function testInvalidLimit()
    {
        $config = [
            'parameters' => [
                'accessKeyId' => 'a',
                '#secretAccessKey' => 'b',
                'bucket' => 'c',
                'key' => 'd',
                'includeSubfolders' => false,
                'newFilesOnly' => false,
                'saveAs' => 'myfile.csv',
                'limit' => -1,
            ],
        ];

        $this->expectException(InvalidConfigurationException::class);

        new Config($config, new ConfigDefinition);
    }

    public function testMissingLimit()
    {
        $config = [
            'parameters' => [
                'accessKeyId' => 'a',
                '#secretAccessKey' => 'b',
                'bucket' => 'c',
                'key' => 'd',
                'includeSubfolders' => false,
                'newFilesOnly' => false,
                'saveAs' => 'myfile.csv',
            ],
        ];

        $this->assertEquals(
            0,
            (new Config($config, new ConfigDefinition))->getLimit()
        );
    }

    /**
     * @dataProvider invalidKeyProvider
     */
    public function testInvalidKey(string $key): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Use the wildcard flag or enter a full path to the file.');

        new Config([
            'parameters' => [
                'accessKeyId' => 'a',
                '#secretAccessKey' => 'b',
                'bucket' => 'c',
                'key' => $key,
            ],
        ], new ConfigDefinition);
    }

    /**
     * @return array
     */
    public function invalidKeyProvider(): array
    {
        return [
            ['foo/bar/'],
            ['FooBar/'],
            ['*/'],
            ['//'],
            ['/'],
        ];
    }
}
