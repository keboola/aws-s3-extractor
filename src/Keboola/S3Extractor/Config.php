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

    public function getExternalId(): string
    {
        return $this->getValue(['parameters', 'externalId']);
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
}
