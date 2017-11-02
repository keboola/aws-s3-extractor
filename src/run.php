<?php

// Catch all warnings and notices
set_error_handler(function ($errno, $errstr, $errfile, $errline, array $errcontext) {
    throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
});

require __DIR__ . '/../vendor/autoload.php';

use Keboola\S3Extractor\Application;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

$dataDir = getenv('KBC_DATADIR') === false ? '/data/' : getenv('KBC_DATADIR');

$configFile = $dataDir . '/config.json';
if (!file_exists($configFile)) {
    echo 'Config file not found' . "\n";
    exit(2);
}

define('ROOT_PATH', __DIR__ . '/..');

try {
    $jsonDecode = new JsonDecode(true);
    $config = $jsonDecode->decode(
        file_get_contents($dataDir . '/config.json'),
        JsonEncoder::FORMAT
    );
    $outputPath = $dataDir . '/out/files';

    $streamHandler = new \Monolog\Handler\StreamHandler('php://stdout');
    $streamHandler->setFormatter(new \Monolog\Formatter\LineFormatter("%message%"));
    $application = new Application($config, $streamHandler);
    $application->actionRun($outputPath);
    exit(0);
} catch (\Symfony\Component\Config\Definition\Exception\InvalidConfigurationException $e) {
    echo "Invalid configuration";
    exit(1);
} catch (\Keboola\S3Extractor\Exception $e) {
    echo $e->getMessage();
    exit(1);
}
