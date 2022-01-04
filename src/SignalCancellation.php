<?php

namespace Amp;

use Revolt\EventLoop;

/**
 * A Signal Cancellation is a way to cancel when the application receives a given signal.
 */
final class SignalCancellation
{
    /**
     * @var \Amp\DeferredCancellation The underlying cancellation manager
     */
    private DeferredCancellation $cancellation;
    private string $handler;

    /**
     * Configures a new cancellation to cancel when the given signal is trapped.
     *
     * @param int $signal
     *
     * @throws \Revolt\EventLoop\UnsupportedFeatureException
     */
    public function __construct(int $signal)
    {
        $this->cancellation = new DeferredCancellation();
        $this->handler = EventLoop::onSignal($signal, $this->cancel(...));
    }

    /**
     * Unreferences the current callback and cancels the cancellation.
     *
     * @param string $handler
     * @param int    $signal
     *
     * @return void
     */
    private function cancel(string $handler, int $signal): void {
        if($this->handler !== $handler) {
            throw new \RuntimeException('Cancel called for the handler it should not handle!');
        }
        EventLoop::unreference($handler);
        $this->cancellation->cancel();
    }

    /**
     * Get the cancellation to give to other functions.
     *
     * @return \Amp\Cancellation
     */
    public function getCancellation(): Cancellation
    {
        return $this->cancellation->getCancellation();
    }
}
