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
$source = $basedir . '/tests/_data';

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
    'folder2/file3/file1.csv'
];
foreach ($files as $file) {
    echo "Transferring {$file}\n";
    $client->putObject([
        'Bucket' => $bucket,
        'Key' => $file,
        'Body' => fopen($source . '/' . $file, 'r')
    ]);
    sleep(1);
}

// Create empty folder
print "Creating /emptyfolder/\n";
$client->putObject([
    'Bucket' => $bucket,
    'Key' => 'emptyfolder/'
]);

echo "Data loaded OK\n";
