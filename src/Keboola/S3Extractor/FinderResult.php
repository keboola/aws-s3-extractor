<?php

namespace Keboola\S3Extractor;

class FinderResult
{
    /**
     * @var iterable|S3File[]
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

    /**
     * @var State
     */
    private $state;

    public function __construct(iterable $iterator, int $count, int $downloadSizeBytes, State $state)
    {
        $this->iterator = $iterator;
        $this->count = $count;
        $this->downloadSizeBytes = $downloadSizeBytes;
        $this->state = $state;
    }

    /**
     * @return S3File[]
     */
    public function getIterator(): iterable
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

    public function getState(): State
    {
        return $this->state;
    }
}
