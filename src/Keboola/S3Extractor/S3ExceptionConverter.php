<?php

namespace Keboola\S3Extractor;

use Aws\S3\Exception\S3Exception;
use GuzzleHttp\Exception\ClientException;

class S3ExceptionConverter
{
    public const ERROR_CODE_SLOW_DOWN = 'SlowDown';
    public const ERROR_CODE_NOT_FOUND_KEY = 'NotFound';

    /**
     * @param S3Exception $e
     * @param string $searchKey
     * @throws Exception
     * @throws S3Exception
     */
    public static function resolve(S3Exception $e, string $searchKey): void
    {
        switch ($e->getStatusCode()) {
            case 403:
                throw new Exception('Invalid credentials or permissions.', $e->getCode(), $e);
                break;
            case 503:
                self::handleServiceUnavailable($e);
                break;
            case 400:
            case 401:
                self::handleBaseUserErrors($e);
                break;
            case 404:
                self::handleNotFound($e, $searchKey);
                break;
            default:
                throw $e;
        }
    }

    /**
     * @param S3Exception $e
     * @throws Exception
     */
    private static function handleServiceUnavailable(S3Exception $e): void
    {
        if ($e->getAwsErrorCode() === self::ERROR_CODE_SLOW_DOWN) {
            throw new Exception('Error 503 Slow Down: The number of requests to the S3 bucket is very high.', $e->getCode(), $e);
        }

        throw new Exception($e->getMessage(), $e->getCode(), $e);
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

    /**
     * @param S3Exception $e
     * @param string $searchKey
     * @throws Exception
     */
    private static function handleNotFound(S3Exception $e, string $searchKey): void
    {
        if ($e->getAwsErrorCode() === self::ERROR_CODE_NOT_FOUND_KEY) {
            throw new Exception(sprintf('Error 404: Key "%s" not found.', $searchKey), $e->getCode(), $e);
        }

        self::handleBaseUserErrors($e);
    }
}
