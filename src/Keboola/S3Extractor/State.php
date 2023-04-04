<?php

namespace Keboola\S3Extractor;

class State
{
    /** @var int */
    public $lastDownloadedFileTimestamp;

    /** @var string[] */
    public $processedFilesInLastTimestampSecond;

    public function __construct(array $state)
    {
        $this->lastDownloadedFileTimestamp = (int)($state['lastDownloadedFileTimestamp'] ?? 0);
        $this->processedFilesInLastTimestampSecond = $state['processedFilesInLastTimestampSecond'] ?? [];
    }
}
