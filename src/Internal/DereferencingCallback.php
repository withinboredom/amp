<?php

namespace Amp\Internal;

final class DereferencingCallback
{
    public function __construct(private readonly \Closure $cancellation)
    {
    }

    public function __destruct()
    {
        ($this->cancellation)();
    }
}
