<?php

namespace Keboola\S3Extractor;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    /**
     * @return string
     */
    public function key(): string
    {
        return $this->getValue(['parameters', 'key']);
    }

    /**
     * @return string
     */
    public function accessKeyId(): string
    {
        return $this->getValue(['parameters', 'accessKeyId']);
    }

    /**
     * @return string
     */
    public function secretAccessKey(): string
    {
        return $this->getValue(['parameters', '#secretAccessKey']);
    }

    /**
     * @return string
     */
    public function bucket(): string
    {
        return $this->getValue(['parameters', 'bucket']);
    }

    /**
     * @return string
     */
    public function saveAs(): string
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
    public function limit(): int
    {
        return $this->getValue(['parameters', 'limit']);
    }
}
