<?php

namespace Amp\Stream;

use Amp\Promise;
use Amp\Stream;
use Amp\StreamSource;
use function Amp\asyncCall;
use function Amp\call;

final class SerialPipeline extends Pipeline
{
    public static function fromStream(Stream $stream): self
    {
        return new self($stream);
    }

    /** @var Stream */
    private $stream;

    /** @var bool */
    private $preserveOrder = true;

    private function __construct(Stream $stream)
    {
        $this->stream = $stream;
    }

    public function apply(callable $operator): Pipeline
    {
        $source = new StreamSource;
        $pipeline = new self($source->stream());
        $pipeline->preserveOrder = $this->preserveOrder;

        asyncCall(function () use ($operator, $source) {
            try {
                while (null !== $value = yield $this->continue()) {
                    yield call($operator, $value, \Closure::fromCallable([$source, 'emit']));
                }

                $source->complete();
            } catch (\Throwable $e) {
                $source->fail($e);
            }
        });

        return $pipeline;
    }

    public function concurrent(int $concurrency): ConcurrentOrderedPipeline
    {
        return ConcurrentOrderedPipeline::fromStream($this, $concurrency);
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
