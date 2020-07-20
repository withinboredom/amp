<?php

namespace Amp\Stream;

use Amp\Promise;
use Amp\Stream;
use Amp\StreamSource;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\delay;
use function Amp\Internal\createTypeError;

/**
 * drop, dropUntil, dropWhile, takeUntil, takeWhile, find, findIndex, firstElement, firstOrDefault, firstOrElse
 * flatMap, foldLeft
 */
abstract class Pipeline implements Stream
{
    abstract public function apply(callable $operator): Pipeline;

    public function map(callable $operator): Pipeline
    {
        return $this->apply(static function ($value, callable $emit) use ($operator) {
            yield $emit(yield call($operator, $value));
        });
    }

    public function filter(callable $operator): Pipeline
    {
        return $this->apply(static function ($value, callable $emit) use ($operator) {
            if (yield call($operator, $value)) {
                yield $emit($value);
            }
        });
    }

    public function limit(int $count): Pipeline
    {
        $elements = 0;

        return $this->apply(static function ($value, callable $emit, callable $stop) use (&$elements, $count) {
            if (++$elements <= $count) {
                yield $emit($value);

                if ($elements === $count) {
                    $stop();
                }
            }
        });
    }

    public function skip(int $count): Pipeline
    {
        $elements = 0;

        return $this->apply(static function ($value, callable $emit) use (&$elements, $count) {
            if (++$elements > $count) {
                yield $emit($value);
            }
        });
    }

    public function delay(int $delayInMilliseconds): Pipeline
    {
        return $this->apply(static function ($value, callable $emit) use ($delayInMilliseconds) {
            yield delay($delayInMilliseconds);
            yield $emit($value);
        });
    }

    public function flat(): Pipeline
    {
        return $this->apply(static function ($value, callable $emit) {
            if (!$value instanceof Stream) {
                throw createTypeError([Stream::class], $value);
            }

            while (null !== $innerValue = yield $value->continue()) {
                yield $emit($innerValue);
            }
        });
    }

    public function mergeSorted(callable $comparator): SerialPipeline
    {
        $source = new StreamSource;

        asyncCall(function () use ($comparator, $source) {
            try {
                $streams = yield $this->toArray();

                if (!$streams) {
                    $source->complete();

                    return;
                }

                $promises = [];
                foreach ($streams as $stream) {
                    $promises[] = $stream->continue();
                }

                $values = yield $promises;

                $sortedKeys = [];
                $sortedValues = [];
                $sortedCount = 0;

                foreach ($values as $key => $value) {
                    if ($value === null) {
                        unset($values[$key]);

                        continue;
                    }

                    if ($sortedCount === 0) {
                        $sortedValues = [$value];
                        $sortedKeys = [$key];
                        $sortedCount = 1;

                        continue;
                    }

                    $insertIndex = 0;

                    foreach ($sortedValues as $index => $sortedValue) {
                        $compare = yield call($comparator, $value, $sortedValue);
                        if ($compare <= 0) {
                            \array_splice($sortedValues, $insertIndex, 0, [$value]);
                            \array_splice($sortedKeys, $insertIndex, 0, [$key]);
                            $sortedCount++;
                            continue 2;
                        }

                        $insertIndex++;
                    }

                    \array_splice($sortedValues, $insertIndex, 0, [$value]);
                    \array_splice($sortedKeys, $insertIndex, 0, [$key]);
                    $sortedCount++;
                }

                while ($sortedCount > 0) {
                    $minKey = $sortedKeys[0];

                    yield $source->emit($sortedValues[0]);

                    unset($sortedValues[0], $sortedKeys[0]);

                    $newValue = yield $streams[$minKey]->continue();
                    if ($newValue !== null) {
                        $insertIndex = 0;
                        foreach ($sortedValues as $sortedValue) {
                            $compare = yield call($comparator, $newValue, $sortedValue);
                            if ($compare <= 0) {
                                \array_splice($sortedValues, $insertIndex, 0, [$newValue]);
                                \array_splice($sortedKeys, $insertIndex, 0, [$minKey]);
                                continue 2;
                            }

                            $insertIndex++;
                        }

                        \array_splice($sortedValues, $insertIndex, 0, [$newValue]);
                        \array_splice($sortedKeys, $insertIndex, 0, [$minKey]);
                    } else {
                        $sortedCount--;
                    }
                }

                $source->complete();
            } catch (\Throwable $e) {
                $source->fail($e);
            }
        });

        return SerialPipeline::fromStream($source->stream());
    }

    public function forEach(callable $operator): Promise
    {
        return discard($this->apply(static function ($value) use ($operator) {
            yield call($operator, $value);
        }));
    }

    public function average(): Promise
    {
        return call(function () {
            $total = 0;
            $count = 0;

            while (null !== $value = yield $this->continue()) {
                if (!\is_numeric($value)) {
                    throw new \TypeError("Non-numeric value encountered: " . \gettype($value));
                }

                $total += $value;
                $count++;
            }

            if ($count === 0) {
                return null;
            }

            return $total / $count;
        });
    }

    public function sum(): Promise
    {
        return call(function () {
            $total = 0;

            while (null !== $value = yield $this->continue()) {
                if (!\is_numeric($value)) {
                    throw new \TypeError("Non-numeric value encountered: " . \gettype($value));
                }

                $total += $value;
            }

            return $total;
        });
    }

    public function count(): Promise
    {
        return call(function () {
            $count = 0;

            while (null !== yield $this->continue()) {
                $count++;
            }

            return $count;
        });
    }

    public function contains($search): Promise
    {
        return call(function () use ($search) {
            while (null !== $value = yield $this->continue()) {
                if ($value === $search) {
                    return true;
                }
            }

            return false;
        });
    }

    public function min(): Promise
    {
        return call(function () {
            $min = null;

            while (null !== $value = yield $this->continue()) {
                if (($value <=> $min) < 0) {
                    $min = $value;
                }
            }

            return $value;
        });
    }

    public function max(): Promise
    {
        return call(function () {
            $max = null;

            while (null !== $value = yield $this->continue()) {
                if (($value <=> $max) > 0) {
                    $max = $value;
                }
            }

            return $value;
        });
    }

    public function reduce(callable $operator, $initial): Promise
    {
        return call(function () use ($operator, $initial) {
            $result = $initial;

            while (null !== $value = yield $this->continue()) {
                $result = yield call($operator, $result, $value);
            }

            return $result;
        });
    }

    public function join(string $separator, string $prefix = '', string $suffix = ''): Promise
    {
        return call(function () use ($separator, $prefix, $suffix) {
            $result = $prefix;
            $first = true;

            while (null !== $value = yield $this->continue()) {
                if (!\is_string($value) && !\is_int($value)) {
                    throw createTypeError(['string', 'int'], $value);
                }

                if (!$first) {
                    $result .= $separator;
                }

                $result .= $value;
            }

            return $result . $suffix;
        });
    }

    public function toArray(): Promise
    {
        return toArray($this);
    }
}
