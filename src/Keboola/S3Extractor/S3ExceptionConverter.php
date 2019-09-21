<?php

namespace Keboola\S3Extractor;

use Aws\S3\Exception\S3Exception;
use GuzzleHttp\Exception\ClientException;

class S3ExceptionConverter
{
    /**
     * @param S3Exception $e
     * @throws Exception
     * @throws S3Exception
     */
    public static function resolve(S3Exception $e): void
    {
        switch ($e->getStatusCode()) {
            case 403:
                throw new Exception('Invalid credentials or permissions.', $e->getCode(), $e);
                break;
            case 503:
                throw new Exception('Error 503 Slow Down: The number of requests to the S3 bucket is very high.', $e->getCode(), $e);
                break;
            case 400:
            case 401:
            case 404:
                self::handleBaseUserErrors($e);
                break;
            default:
                throw $e;
        }
    }

    /**
     * @param S3Exception $e
     * @throws Exception
     */
    private static function handleBaseUserErrors(S3Exception $e): void
    {
        if ($e->getPrevious() && get_class($e->getPrevious()) === ClientException::class) {
            /** @var ClientException $previous */
            $previous = $e->getPrevious();
            if ($previous->getResponse()) {
                throw new Exception(
                    $previous->getResponse()->getStatusCode()
                    . " "
                    . $previous->getResponse()->getReasonPhrase()
                    . " ("
                    . $e->getAwsErrorCode()
                    . ")\n"
                    . $previous->getResponse()->getBody()->__toString()
                );
            }
            throw new Exception($previous->getMessage());
        }
        throw new Exception($e->getMessage());
    }
}
