<?php

require __DIR__ . '/../vendor/autoload.php';

use Keboola\Component\Logger;
use Keboola\Component\UserException;
use Keboola\S3Extractor\Application;

$logger = new Logger;
try {
    (new Application($logger))->execute();
    exit(0);
} catch (UserException $e) {
    $logger->error($e->getMessage());
    exit(1);
} catch (\Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
