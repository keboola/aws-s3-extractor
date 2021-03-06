<?php

namespace Keboola\S3Extractor;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    /**
     * @return string
     */
    public function getKey(): string
    {
        return $this->getValue(['parameters', 'key']);
    }

    /**
     * @return string
     */
    public function getAccessKeyId(): string
    {
        return $this->getValue(['parameters', 'accessKeyId']);
    }

    /**
     * @return string
     */
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

    /**
     * @return string
     */
    public function getBucket(): string
    {
        return $this->getValue(['parameters', 'bucket']);
    }

    /**
     * @return string
     */
    public function getSaveAs(): string
    {
        return $this->getValue(['parameters', 'saveAs']);
    }

    /**
     * @return bool
     */
    public function isIncludeSubfolders(): bool
    {
        return $this->getValue(['parameters', 'includeSubfolders']);
    }

    /**
     * @return bool
     */
    public function isNewFilesOnly(): bool
    {
        return $this->getValue(['parameters', 'newFilesOnly']);
    }

    /**
     * @return int
     */
    public function getLimit(): int
    {
        return $this->getValue(['parameters', 'limit']);
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
        $secretKey = getenv('KEBOOLA_USER_AWS_SECRET_KEY');
        if ($secretKey) {
            return $secretKey;
        }
        if (!isset($this->getImageParameters()['#KEBOOLA_USER_AWS_SECRET_KEY'])) {
            throw new \Exception('Keboola aws user secret key is missing from image parameters');
        }
        return $this->getImageParameters()['#KEBOOLA_USER_AWS_SECRET_KEY'];
    }
}
