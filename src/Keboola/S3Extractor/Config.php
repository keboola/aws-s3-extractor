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
    public function isUncludeSubfolders(): bool
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
