<?php
/**
 * Loads test fixtures into S3
 */

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', true);
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';

$client =  new \Aws\S3\S3Client([
    'region' => getenv('AWS_REGION'),
    'version' => '2006-03-01',
    'credentials' => [
        'key' => getenv('UPLOAD_USER_AWS_ACCESS_KEY'),
        'secret' => getenv('UPLOAD_USER_AWS_SECRET_KEY'),
    ],
]);

// Where the files will be source from
$source = $basedir . '/tests/_S3InitData';

// Where the files will be transferred to
$bucket = getenv('AWS_S3_BUCKET');
$dest = 's3://' . $bucket;

// clear bucket
$result = $client->listObjects([
    'Bucket' => $bucket,
]);

$objects = $result->get('Contents');
if ($objects) {
    $client->deleteObjects([
        'Bucket' => $bucket,
        'Delete' => [
            'Objects' => array_map(function ($object) {
                return [
                    'Key' => $object['Key'],
                ];
            }, $objects),
        ],
    ]);
}

// Manually transfer files
$files = [
    'collision-file1.csv',
    'file1.csv',
    'collision/file1.csv',
    'folder1/file1.csv',
    'folder2/collision-file1.csv',
    'folder2/file1.csv',
    'folder2/file2.csv',
    'folder2/collision/file1.csv',
    'folder2/file3/file1.csv',
];
foreach ($files as $file) {
    echo "Transferring {$bucket}/{$file} slowly\n";
    $client->putObject([
        'Bucket' => $bucket,
        'Key' => $file,
        'Body' => fopen($source . '/' . $file, 'r')
    ]);
    // This ensures each file has a unique timestamp
    sleep(1);
}

$file = 'snappy-compressed/snappy_compressed_data.orc';
echo "Transferring {$bucket}/snappy-compressed/{$file}\n";
$client->putObject(
    [
        'Bucket' => $bucket,
        'Key' => $file,
        'Body' => fopen($source . '/' . $file, 'r'),
        'ContentType' => 'hadoop-snappy',
        'ContentEncoding' => 'x-snappy-framed'
    ]
);
sleep(1);

// Manually transfer files
$files = [
    'folder2/collision-file1.csv',
    'folder2/file1.csv',
    'folder2/file2.csv',
    'folder2/collision/file1.csv'
];

$equalTimestamp = false;
do {
    foreach ($files as $file) {
        echo "Transferring {$bucket}/no-unique-timestamps/{$file} quickly\n";
        $client->putObject(
            [
                'Bucket' => $bucket,
                'Key' => 'no-unique-timestamps/' . $file,
                'Body' => fopen($source . '/' . $file, 'r')
            ]
        );
    }
    sleep(1);
    // check timestamps if all of them are equal
    $timestamps = [];
    foreach ($files as $file) {
        $parameters = [
            'Bucket' => $bucket,
            'Key' => 'no-unique-timestamps/' . $file
        ];
        $timestamps[] = $client->headObject($parameters)["LastModified"]->format("U");
    }
    if (count(array_unique($timestamps)) === 1) {
        $equalTimestamp = true;
    } else {
        echo "Timestamps not equal, retrying\n";
    }
} while (!$equalTimestamp);

sleep(1);
$file = 'folder2/file3/file1.csv';
echo "Transferring {$bucket}/no-unique-timestamps/{$file} after a while\n";
$client->putObject(
    [
        'Bucket' => $bucket,
        'Key' => 'no-unique-timestamps/' . $file,
        'Body' => fopen($source . '/' . $file, 'r')
    ]
);

// Create an empty folder
print "Creating /emptyfolder/\n";
$client->putObject([
    'Bucket' => $bucket,
    'Key' => 'emptyfolder/'
]);

$file = 'snappy-compressed/snappy_compressed_data.orc';
echo "Transferring {$bucket}/snappy-compressed/{$file}\n";
$client->putObject(
    [
        'Bucket' => $bucket,
        'Key' => $file,
        'Body' => fopen($source . '/' . $file, 'r'),
        'ContentType' => 'hadoop-snappy',
        'ContentEncoding' => 'x-snappy-framed'
    ]
);

echo "Data loaded OK\n";
