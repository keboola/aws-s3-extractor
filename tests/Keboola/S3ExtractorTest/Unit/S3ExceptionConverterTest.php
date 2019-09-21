<?php

namespace Keboola\S3ExtractorTest\Unit;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Keboola\S3Extractor\Exception;
use Keboola\S3Extractor\S3ExceptionConverter;
use Aws\S3\Exception\S3Exception;

class S3ExceptionConverterTest extends TestCase
{
    public function testInvalidCredentials(): void
    {
        $exception = $this->mockS3Exception(403);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid credentials or permissions.');
        /** @var S3Exception $exception */
        S3ExceptionConverter::resolve($exception);
    }

    public function testSlowDown(): void
    {
        $exception = $this->mockS3Exception(503);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Error 503 Slow Down: The number of requests to the S3 bucket is very high.');
        /** @var S3Exception $exception */
        S3ExceptionConverter::resolve($exception);
    }

    /**
     * @dataProvider basicUserErrorProvider
     */
    public function testBaseUserException(int $statusCode): void
    {
        $exception = $this->mockS3Exception($statusCode);
        $this->expectException(Exception::class);
        /** @var S3Exception $exception */
        S3ExceptionConverter::resolve($exception);
    }

    public function testS3Exception(): void
    {
        $exception = $this->mockS3Exception();
        $this->expectException(S3Exception::class);
        /** @var S3Exception $exception */
        S3ExceptionConverter::resolve($exception);
    }

    public function basicUserErrorProvider(): array
    {
        return [
            [400],
            [401],
            [404],
        ];
    }

    /**
     * @param int|null $statusCode
     * @return MockObject
     */
    public function mockS3Exception(?int $statusCode = null): MockObject
    {
        $exception = $this->createMock(S3Exception::class);
        $exception->method('getStatusCode')
            ->willReturn($statusCode);

        return $exception;
    }
}
