<?php

namespace Keboola\S3ExtractorTest\Functional;

use Keboola\S3Extractor\Config;
use Keboola\S3Extractor\ConfigDefinition;
use Keboola\S3Extractor\Extractor;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class NewFilesOnlyEqualTimestampFunctionalTest extends FunctionalTestCase
{
    /**
     * @param $testFile
     * @param TestHandler $testHandler
     */
    private function assertFileDownloadedFromS3($testFile, TestHandler $testHandler)
    {
        $testFileReplaced = str_replace('/', '-', str_replace('-', '--', substr($testFile, 1)));
        $this->assertFileExists($this->path . '/' . $testFileReplaced);
        $this->assertFileEquals(
            __DIR__ . "/../../../_data" . $testFile,
            $this->path . '/' . $testFileReplaced
        );
        $this->assertTrue($testHandler->hasInfo("Downloading file /no-unique-timestamps{$testFile}"));
    }

    /**
     * @param TestHandler $testHandler
     * @param array $state
     * @return array
     */
    private function runExtraction(TestHandler $testHandler, $state = [])
    {
        $key = "no-unique-timestamps/*";
        $extractor = new Extractor(new Config(
            [
                "parameters" => [
                    "accessKeyId" => getenv(self::AWS_S3_ACCESS_KEY_ENV),
                    "#secretAccessKey" => getenv(self::AWS_S3_SECRET_KEY_ENV),
                    "bucket" => getenv(self::AWS_S3_BUCKET_ENV),
                    "key" => $key,
                    "includeSubfolders" => true,
                    "newFilesOnly" => true,
                    "limit" => 1,
                ],
            ], new ConfigDefinition),
            $state,
            (new Logger('test'))->pushHandler($testHandler)
        );
        return $extractor->extract($this->path);
    }

    /**
     *
     */
    public function testSuccessfulDownloadFromFolderContinuously()
    {
        // Download sequentially 4 files with the same timestamp

        $testHandler = new TestHandler();
        $state1 = $this->runExtraction($testHandler, []);

        $this->assertTrue($testHandler->hasInfo("Downloading only 1 oldest file(s) out of 5"));
        $this->assertFileDownloadedFromS3('/folder2/collision-file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state1);
        $this->assertArrayHasKey('processedFilesInLastTimestampSecond', $state1);
        $this->assertGreaterThan(0, $state1['lastDownloadedFileTimestamp']);
        $this->assertCount(1, $state1['processedFilesInLastTimestampSecond']);
        $this->assertEquals(
            ['no-unique-timestamps/folder2/collision-file1.csv'],
            $state1['processedFilesInLastTimestampSecond']
        );

        $testHandler = new TestHandler();
        $state2 = $this->runExtraction($testHandler, $state1);

        $this->assertTrue($testHandler->hasInfo("Downloading only 1 oldest file(s) out of 4"));
        $this->assertFileDownloadedFromS3('/folder2/collision/file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state2);
        $this->assertArrayHasKey('processedFilesInLastTimestampSecond', $state2);
        $this->assertGreaterThan(0, $state2['lastDownloadedFileTimestamp']);
        $this->assertCount(2, $state2['processedFilesInLastTimestampSecond']);
        $this->assertEquals(
            ['no-unique-timestamps/folder2/collision-file1.csv', 'no-unique-timestamps/folder2/collision/file1.csv'],
            $state2['processedFilesInLastTimestampSecond']
        );
        $this->assertEquals($state1['lastDownloadedFileTimestamp'], $state2['lastDownloadedFileTimestamp']);

        $testHandler = new TestHandler();
        $state3 = $this->runExtraction($testHandler, $state2);

        $this->assertTrue($testHandler->hasInfo("Downloading only 1 oldest file(s) out of 3"));
        $this->assertFileDownloadedFromS3('/folder2/file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state3);
        $this->assertArrayHasKey('processedFilesInLastTimestampSecond', $state3);
        $this->assertGreaterThan(0, $state3['lastDownloadedFileTimestamp']);
        $this->assertCount(3, $state3['processedFilesInLastTimestampSecond']);
        $this->assertEquals(
            [
                'no-unique-timestamps/folder2/collision-file1.csv',
                'no-unique-timestamps/folder2/collision/file1.csv',
                'no-unique-timestamps/folder2/file1.csv',
            ],
            $state3['processedFilesInLastTimestampSecond']
        );
        $this->assertEquals($state1['lastDownloadedFileTimestamp'], $state3['lastDownloadedFileTimestamp']);

        $testHandler = new TestHandler();
        $state4 = $this->runExtraction($testHandler, $state3);

        $this->assertTrue($testHandler->hasInfo("Downloading only 1 oldest file(s) out of 2"));
        $this->assertFileDownloadedFromS3('/folder2/file2.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(3, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state4);
        $this->assertArrayHasKey('processedFilesInLastTimestampSecond', $state4);
        $this->assertGreaterThan(0, $state4['lastDownloadedFileTimestamp']);
        $this->assertCount(4, $state4['processedFilesInLastTimestampSecond']);
        $this->assertEquals(
            [
                'no-unique-timestamps/folder2/collision-file1.csv',
                'no-unique-timestamps/folder2/collision/file1.csv',
                'no-unique-timestamps/folder2/file1.csv',
                'no-unique-timestamps/folder2/file2.csv',
            ],
            $state4['processedFilesInLastTimestampSecond']
        );
        $this->assertEquals($state1['lastDownloadedFileTimestamp'], $state4['lastDownloadedFileTimestamp']);

        // Files with equal timestamp are done, new file should proceed

        $testHandler = new TestHandler();
        $state5 = $this->runExtraction($testHandler, $state4);

        $this->assertFileDownloadedFromS3('/folder2/file3/file1.csv', $testHandler);
        $this->assertTrue($testHandler->hasInfo("Downloaded 1 file(s)"));
        $this->assertCount(2, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state5);
        $this->assertArrayHasKey('processedFilesInLastTimestampSecond', $state5);
        $this->assertGreaterThan(0, $state5['lastDownloadedFileTimestamp']);
        $this->assertCount(1, $state5['processedFilesInLastTimestampSecond']);
        $this->assertEquals(
            ['no-unique-timestamps/folder2/file3/file1.csv'],
            $state5['processedFilesInLastTimestampSecond']
        );
        $this->assertNotEquals($state1['lastDownloadedFileTimestamp'], $state5['lastDownloadedFileTimestamp']);

        // All files done, nothing to do here, just keep state

        $testHandler = new TestHandler();
        $state6 = $this->runExtraction($testHandler, $state5);

        $this->assertTrue($testHandler->hasInfo("Downloaded 0 file(s)"));
        $this->assertCount(1, $testHandler->getRecords());
        $this->assertArrayHasKey('lastDownloadedFileTimestamp', $state6);
        $this->assertArrayHasKey('processedFilesInLastTimestampSecond', $state6);
        $this->assertGreaterThan(0, $state6['lastDownloadedFileTimestamp']);
        $this->assertCount(1, $state6['processedFilesInLastTimestampSecond']);
        $this->assertEquals(
            ['no-unique-timestamps/folder2/file3/file1.csv'],
            $state6['processedFilesInLastTimestampSecond']
        );
        $this->assertEquals($state5['lastDownloadedFileTimestamp'], $state6['lastDownloadedFileTimestamp']);
    }
}
