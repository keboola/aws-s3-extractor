<?php

namespace Keboola\S3Extractor;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getKey(): string
    {
        /** @var string $value */
        $value = $this->getValue(['parameters', 'key']);
        if (substr($value, 0, 1) == '/') {
            $value = substr($value, 1);
        }
        return $value;
    }

    public function getAccessKeyId(): string
    {
        return $this->getValue(['parameters', 'accessKeyId']);
    }

    public function getSecretAccessKey(): string
    {
        return $this->getValue(['parameters', '#secretAccessKey']);
    }

    public function getLoginType(): string
    {
        return $this->getValue(['parameters', 'loginType']);
    }

    public function getAccountId(): string
    {
        return $this->getValue(['parameters', 'accountId']);
    }

    public function getRoleName(): string
    {
        return $this->getValue(['parameters', 'roleName']);
    }

    public function getBucket(): string
    {
        return $this->getValue(['parameters', 'bucket']);
    }

    public function getSaveAs(): string
    {
        return $this->getValue(['parameters', 'saveAs']);
    }

    public function isIncludeSubfolders(): bool
    {
        return $this->getValue(['parameters', 'includeSubfolders']);
    }

    public function isNewFilesOnly(): bool
    {
        return $this->getValue(['parameters', 'newFilesOnly']);
    }

    public function getLimit(): int
    {
        return $this->getValue(['parameters', 'limit']);
    }

    public function hasDecodeContent(): bool
    {
        return $this->getValue(['parameters', 'decodeContent']);
    }

    public function getKeboolaUserAwsAccessKey(): string
    {
        $accessKey = getenv('KEBOOLA_USER_AWS_ACCESS_KEY');
        if ($accessKey) {
            return $accessKey;
        }
        if (!isset($this->getImageParameters()['KEBOOLA_USER_AWS_ACCESS_KEY'])) {
            throw new \Exception('Keboola aws user access key is missing from image parameters');
        }
        return $this->getImageParameters()['KEBOOLA_USER_AWS_ACCESS_KEY'];
    }

    public function getKeboolaUserAwsSecretKey(): string
    {
        $secretKey = (string) getenv('KEBOOLA_USER_AWS_SECRET_KEY');
        if ($secretKey) {
            return $secretKey;
        }
        if (!isset($this->getImageParameters()['#KEBOOLA_USER_AWS_SECRET_KEY'])) {
            throw new \Exception('Keboola aws user secret key is missing from image parameters');
        }
        return $this->getImageParameters()['#KEBOOLA_USER_AWS_SECRET_KEY'];
    }
}
