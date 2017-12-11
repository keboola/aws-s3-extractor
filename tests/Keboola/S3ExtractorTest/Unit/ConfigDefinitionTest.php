<?php

namespace Keboola\S3ExtractorTest\Unit;

use Keboola\S3Extractor\ConfigDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

class ConfigDefinitionTest extends TestCase
{
    public function testValidConfig()
    {
        $json = <<<JSON
{
    "parameters": {
        "accessKeyId": "a",
        "#secretAccessKey": "b",
        "bucket": "c",
        "key": "d",
        "includeSubfolders": false,
        "newFilesOnly": false,
        "saveAs": "myfile.csv",
        "limit": 1
    }
}
JSON;

        $config = (new JsonDecode(true))->decode($json, JsonEncoder::FORMAT);
        $processor = new Processor;
        $processedConfig = $processor->processConfiguration(new ConfigDefinition(), [$config['parameters']]);

        $this->assertInternalType('array', $processedConfig);
    }

    public function testInvalidConfig()
    {
        $this->expectException(InvalidConfigurationException::class);

        $json = <<<JSON
{
    "parameters": {
        "accessKeyId": "a",
        "#secretAccessKey": "b",
        "bucket": "c"
    }   
}
JSON;

        $config = (new JsonDecode(true))->decode($json, JsonEncoder::FORMAT);
        (new Processor())->processConfiguration(new ConfigDefinition, [$config['parameters']]);
    }

    public function testInvalidLimit()
    {
        $this->expectException(InvalidConfigurationException::class);

        $json = <<<JSON
{
    "parameters": {
        "accessKeyId": "a",
        "#secretAccessKey": "b",
        "bucket": "c",
        "key": "d",
        "includeSubfolders": false,
        "newFilesOnly": false,
        "saveAs": "myfile.csv",
        "limit": 0
    }   
}
JSON;

        $config = (new JsonDecode(true))->decode($json, JsonEncoder::FORMAT);
        (new Processor())->processConfiguration(new ConfigDefinition, [$config['parameters']]);
    }
    
    public function testMissingLimit()
    {
        $json = <<<JSON
{
    "parameters": {
        "accessKeyId": "a",
        "#secretAccessKey": "b",
        "bucket": "c",
        "key": "d",
        "includeSubfolders": false,
        "newFilesOnly": false,
        "saveAs": "myfile.csv"
    }   
}
JSON;

        $config = (new JsonDecode(true))->decode($json, JsonEncoder::FORMAT);
        $parameters = (new Processor())->processConfiguration(new ConfigDefinition, [$config['parameters']]);
        $this->assertEquals(1000, $parameters["limit"]);
    }
}
