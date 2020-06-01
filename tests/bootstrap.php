<?php

use Symfony\Component\Process\Process;

require __DIR__ . '/../vendor/autoload.php';
define('ROOT_PATH', __DIR__ . '/..');

$process = Process::fromShellCommandline(sprintf('php %s/loadS3.php', __DIR__));
$process
    ->setTimeout(2000)
    ->mustRun();
