<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\Component\JsonHelper;
use Symfony\Component\Filesystem\Exception\FileNotFoundException;
use Symfony\Component\Process\Process;

class FunctionalTestCase extends \PHPUnit\Framework\TestCase
{
    const AWS_S3_BUCKET_ENV = 'AWS_S3_BUCKET';
    const AWS_S3_ACCESS_KEY_ENV = 'DOWNLOAD_USER_AWS_ACCESS_KEY';
    const AWS_S3_SECRET_KEY_ENV = 'DOWNLOAD_USER_AWS_SECRET_KEY';
    const UPDATE_AWS_S3_ACCESS_KEY_ENV = 'UPLOAD_USER_AWS_ACCESS_KEY';
    const UPDATE_AWS_S3_SECRET_KEY_ENV = 'UPLOAD_USER_AWS_SECRET_KEY';
    const UPDATE_AWS_S3_BUCKET = 'AWS_S3_BUCKET';
    const UPDATE_AWS_REGION = 'AWS_REGION';
    protected $path;

    public static function setUpBeforeClass()
    {
        parent::setUpBeforeClass();
        (new Process('php ' . __DIR__ . '/../../../loadS3.php'))
            ->setTimeout(1000)
            ->mustRun();
    }

    public function setUp()
    {
        $this->path = '/tmp/aws-s3-extractor/' . uniqid();
        mkdir($this->path, 0777, true);
    }

    public function tearDown()
    {
        passthru('rm -rf ' . $this->path);
    }

    protected function writeConfig(array $config): void
    {
        JsonHelper::writeFile($this->path . '/config.json', $config);
    }

    protected function getOutputSatet(): array
    {
        try {
            return JsonHelper::readFile($this->path . '/out/state.json');
        } catch (FileNotFoundException $exception) {
            return [];
        }
    }

    protected function syncInputSatet(): void
    {
        JsonHelper::writeFile($this->path . '/in/state.json', $this->getOutputSatet());
    }
}
