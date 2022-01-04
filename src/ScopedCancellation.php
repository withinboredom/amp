<?php

namespace Amp;

use Amp\Internal\DereferencingCallback;
use Amp\Internal\WrappedCancellation;

/**
 * Creates a cancellation once the $scope variable goes out of scope or loses reference.
 * This can be handy when you expect an uncaught exception and want to make sure you cancel things without writing a
 * try-finally.
 */
class ScopedCancellation
{
    private Internal\Cancellable $source;
    private Cancellation $cancellation;

    public function __construct(&$scope)
    {
        $scope = new DereferencingCallback($this->cancel(...));
        $this->source = new Internal\Cancellable();
        $this->cancellation = new WrappedCancellation($this->source);
    }

    public function getCancellation(): Cancellation
    {
        return $this->cancellation;
    }

    private function cancel(): void
    {
        $this->source->cancel();
    }
}
