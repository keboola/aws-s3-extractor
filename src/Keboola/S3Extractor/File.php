<?php

namespace Keboola\S3Extractor;

/**
 * File class is a container for file metadata.
 */
class File
{
    /** @var string */
    private $bucket;

    /** @var string */
    private $key;

    /** @var int */
    private $timestamp;

    /** @var int */
    private $sizeBytes;

    /** @var string */
    private $saveAs;

    public function __construct(string $bucket, string $key, \DateTimeInterface $lastModified, int $size, string $saveAs)
    {
        $this->bucket = $bucket;
        $this->key = $key;
        $this->timestamp = (int)$lastModified->format("U");
        $this->sizeBytes = $size;
        $this->saveAs = $saveAs;
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getTimestamp(): int
    {
        return $this->timestamp;
    }

    public function getSizeBytes(): int
    {
        return $this->sizeBytes;
    }

    public function getParameters(string $outputDir): array
    {
        return [
            'Bucket' => $this->bucket,
            'Key' => $this->key,
            'SaveAs' => $outputDir . '/'  . $this->saveAs
        ];
    }
}
