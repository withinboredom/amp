<?php

namespace Amp\Stream;

use Amp\Promise;
use Amp\Stream;
use Amp\StreamSource;
use Amp\Success;
use function Amp\asyncCall;
use function Amp\call;

final class ConcurrentOrderedPipeline extends Pipeline
{
    public static function fromStream(Stream $stream, int $concurrency): self
    {
        return new self($stream, $concurrency);
    }

    /** @var Stream */
    private $stream;

    /** @var int */
    private $concurrency;

    private function __construct(Stream $stream, int $concurrency)
    {
        $this->stream = $stream;
        $this->concurrency = $concurrency;
    }

    public function apply(callable $operator): Pipeline
    {
        $previous = new Success;
        $source = new StreamSource;
        $pipeline = new self($source->stream(), $this->concurrency);

        for ($i = 0; $i < $this->concurrency; $i++) {
            asyncCall(function () use ($operator, $source, &$previous) {
                try {
                    next:
                    $promise = $this->continue();

                    $emitPromise = new Success;
                    $emit = static function ($value) use (&$emitPromise, $source, $previous) {
                        $emitPromise = $source->emit($value);

                        return call(static function () use ($previous, $emitPromise) {
                            yield $previous;
                            yield $emitPromise;
                        });
                    };

                    $previous = call(static function () use (&$previous, $promise, &$emitPromise) {
                        yield $previous;
                        yield $promise;
                        yield $emitPromise;
                    });

                    if (null !== $value = yield $promise) {
                        yield call($operator, $value, $emit);

                        goto next;
                    }

                    $source->complete();
                } catch (\Throwable $e) {
                    $source->fail($e);
                }
            });
        }

        return $pipeline;
    }

    public function unordered(): ConcurrentUnorderedPipeline
    {
        return ConcurrentUnorderedPipeline::fromStream($this, $this->concurrency);
    }

    public function sequential(): SerialPipeline
    {
        return SerialPipeline::fromStream($this);
    }

    public function continue(): Promise
    {
        return $this->stream->continue();
    }

    public function dispose()
    {
        $this->stream->dispose();
    }
}
