package io.alerting.engine;

/**
 * A single-threaded task queue with a clock and delayed tasks. Everything that touches one shard's state
 * runs on that shard's scheduler, so per-key processing needs no locks.
 */
public interface Scheduler {
    long now();

    void execute(Runnable task);

    Cancellable schedule(Runnable task, long delayMs);

    void shutdown();

    interface Cancellable {
        void cancel();
    }
}
