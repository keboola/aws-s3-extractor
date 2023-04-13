<?php

namespace Keboola\S3Extractor;

class State
{
    /** @var int */
    public $lastTimestamp;

    /** @var string[] */
    public $filesInLastTimestamp;

    public function __construct(array $state)
    {
        $this->lastTimestamp = (int)($state['lastDownloadedFileTimestamp'] ?? 0);
        $this->filesInLastTimestamp = $state['processedFilesInLastTimestampSecond'] ?? [];
    }

    public function toArray(): array
    {
        return [
            'lastDownloadedFileTimestamp' => (string)$this->lastTimestamp,
            'processedFilesInLastTimestampSecond' => $this->filesInLastTimestamp,
        ];
    }
}
