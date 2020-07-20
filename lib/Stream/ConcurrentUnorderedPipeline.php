<?php

namespace Amp\Stream;

use Amp\Promise;
use Amp\Stream;
use Amp\StreamSource;
use function Amp\asyncCall;
use function Amp\call;

final class ConcurrentUnorderedPipeline extends Pipeline
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
        $source = new StreamSource;
        $pipeline = new self($source->stream(), $this->concurrency);

        $stopped = false;
        $stop = static function () use (&$stopped) {
            $stopped = true;
        };

        $promises = [];

        for ($i = 0; $i < $this->concurrency; $i++) {
            $promises[] = call(function () use ($operator, $source, &$stopped, $stop) {
                while (!$stopped && null !== $value = yield $this->continue()) {
                    yield call($operator, $value, \Closure::fromCallable([$source, 'emit']), $stop);
                }
            });
        }

        asyncCall(static function () use ($promises, $source) {
            try {
                yield $promises;

                $source->complete();
            } catch (\Throwable $e) {
                $source->fail($e);
            }
        });

        return $pipeline;
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
