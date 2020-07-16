<?php

namespace Amp\Stream;

use Amp\Promise;
use Amp\Stream;
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

    public function take(int $count): Pipeline
    {
        $elements = 0;

        return $this->apply(static function ($value, callable $emit) use (&$elements, $count) {
            if (++$elements <= $count) {
                yield $emit($value);
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
