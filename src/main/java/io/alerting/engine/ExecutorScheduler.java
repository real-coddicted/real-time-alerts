package io.alerting.engine;

import java.time.Clock;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Production scheduler: one daemon thread, wall clock. */
public final class ExecutorScheduler implements Scheduler {
    private static final System.Logger LOG = System.getLogger(ExecutorScheduler.class.getName());

    private final ScheduledThreadPoolExecutor executor;
    private final Clock clock;

    public ExecutorScheduler(String threadName, Clock clock) {
        this.clock = clock;
        this.executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
    }

    @Override
    public long now() {
        return clock.millis();
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(guard(task));
    }

    @Override
    public Cancellable schedule(Runnable task, long delayMs) {
        ScheduledFuture<?> f = executor.schedule(guard(task), Math.max(0, delayMs), TimeUnit.MILLISECONDS);
        return () -> f.cancel(false);
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
    }

    private static Runnable guard(Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Throwable t) {
                LOG.log(System.Logger.Level.ERROR, "task failed", t);
            }
        };
    }
}
