<?php

namespace Keboola\S3Extractor;

class FinderResult
{
    /**
     * @var \Iterator|File[]
     */
    private $iterator;

    /**
     * @var int
     */
    private $count;

    /**
     * @var int
     */
    private $downloadSizeBytes;

    public function __construct(\Iterator $iterator, int $count, int $downloadSizeBytes)
    {
        $this->iterator = $iterator;
        $this->count = $count;
        $this->downloadSizeBytes = $downloadSizeBytes;
    }

    /**
     * @return \Iterator|File[]
     */
    public function getIterator(): \Iterator
    {
        return $this->iterator;
    }

    public function getCount(): int
    {
        return $this->count;
    }

    public function getDownloadSizeBytes(): int
    {
        return $this->downloadSizeBytes;
    }
}
