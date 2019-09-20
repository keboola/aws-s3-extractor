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
        self::handleInvalidCredentials($e);
        self::handleSlowDown($e);
        self::handleBaseUserErrors($e);

        throw $e;
    }

    /**
     * @param S3Exception $e
     * @throws Exception
     */
    private static function handleInvalidCredentials(S3Exception $e): void
    {
        if ($e->getStatusCode() === 403) {
            throw new Exception('Invalid credentials or permissions.', $e->getCode(), $e);
        }
    }

    /**
     * @param S3Exception $e
     * @throws Exception
     */
    private static function handleSlowDown(S3Exception $e): void
    {
        if ($e->getStatusCode() === 503) {
            throw new Exception('Error 503 Slow Down: The number of requests to the S3 bucket is very high, please check your bucket limit.', $e->getCode(), $e);
        }
    }

    /**
     * @param S3Exception $e
     * @throws Exception
     */
    private static function handleBaseUserErrors(S3Exception $e): void
    {
        if (false === in_array($e->getStatusCode(), [400, 401, 404], true)) {
            return;
        }

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
