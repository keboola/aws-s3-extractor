<?php

namespace Keboola\S3Extractor;

use Aws\S3\Exception\S3Exception;
use GuzzleHttp\Exception\ClientException;
use Keboola\Component\UserException;

class S3ExceptionConverter
{
    public const ERROR_CODE_SLOW_DOWN = 'SlowDown';
    public const ERROR_CODE_NOT_FOUND_KEY = 'NotFound';

    /**
     * @param S3Exception $e
     * @param string $searchKey
     * @throws UserException
     * @throws S3Exception
     */
    public static function resolve(S3Exception $e, string $searchKey): void
    {
        switch ($e->getStatusCode()) {
            case 403:
                throw new UserException('Invalid credentials or permissions.', $e->getCode(), $e);
            case 503:
                self::handleServiceUnavailable($e);
            case 400:
            case 401:
                self::handleBaseUserErrors($e);
            case 404:
                self::handleNotFound($e, $searchKey);
                break;
            default:
                throw $e;
        }
    }

    /**
     * @param S3Exception $e
     * @throws UserException
     */
    private static function handleServiceUnavailable(S3Exception $e): void
    {
        if ($e->getAwsErrorCode() === self::ERROR_CODE_SLOW_DOWN) {
            throw new UserException('Error 503 Slow Down: The number of requests to the S3 bucket is very high.', $e->getCode(), $e);
        }

        throw new UserException($e->getMessage(), $e->getCode(), $e);
    }

    /**
     * @param S3Exception $e
     * @throws UserException
     */
    private static function handleBaseUserErrors(S3Exception $e): void
    {
        if ($e->getPrevious() && get_class($e->getPrevious()) === ClientException::class) {
            /** @var ClientException $previous */
            $previous = $e->getPrevious();
            if ($previous->getResponse()) {
                throw new UserException(
                    $previous->getResponse()->getStatusCode()
                    . " "
                    . $previous->getResponse()->getReasonPhrase()
                    . " ("
                    . $e->getAwsErrorCode()
                    . ")\n"
                    . $previous->getResponse()->getBody()->__toString()
                );
            }
            throw new UserException($previous->getMessage());
        }
        throw new UserException($e->getMessage());
    }

    /**
     * @param S3Exception $e
     * @param string $searchKey
     * @throws UserException
     */
    private static function handleNotFound(S3Exception $e, string $searchKey): void
    {
        if ($e->getAwsErrorCode() === self::ERROR_CODE_NOT_FOUND_KEY) {
            throw new UserException(sprintf('Error 404: Key "%s" not found.', $searchKey), $e->getCode(), $e);
        }

        self::handleBaseUserErrors($e);
    }
}
