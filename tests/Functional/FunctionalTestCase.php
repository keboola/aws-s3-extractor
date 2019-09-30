<?php

declare(strict_types=1);

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\DatadirTests\AbstractDatadirTestCase;
use Symfony\Component\Process\Process;

class FunctionalTestCase extends AbstractDatadirTestCase
{
    protected const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    protected const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    protected const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';
    protected const UPDATE_AWS_S3_ACCESS_KEY_ENV = 'UPLOAD_USER_AWS_ACCESS_KEY';
    protected const UPDATE_AWS_S3_SECRET_KEY_ENV = 'UPLOAD_USER_AWS_SECRET_KEY';
    protected const UPDATE_AWS_S3_BUCKET = 'AWS_S3_BUCKET';
    protected const UPDATE_AWS_REGION = 'AWS_REGION';

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
//        (new Process('php ' . __DIR__ . '/../loadS3.php'))
//            ->setTimeout(1000)
//            ->mustRun();
    }
}
