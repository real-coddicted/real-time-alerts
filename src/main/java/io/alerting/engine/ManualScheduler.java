package io.alerting.engine;

import java.util.PriorityQueue;

/**
 * Deterministic virtual-time scheduler for tests and replays. Nothing runs until {@link #advance} or
 * {@link #runPending} is called; tasks run in (time, submission) order on the calling thread.
 */
public final class ManualScheduler implements Scheduler {
    private final PriorityQueue<Task> queue = new PriorityQueue<>();
    private long now;
    private long seq;

    public ManualScheduler(long startMillis) {
        this.now = startMillis;
    }

    private static final class Task implements Comparable<Task> {
        final long time;
        final long seq;
        final Runnable runnable;
        boolean cancelled;

        Task(long time, long seq, Runnable runnable) {
            this.time = time;
            this.seq = seq;
            this.runnable = runnable;
        }

        @Override
        public int compareTo(Task o) {
            int c = Long.compare(time, o.time);
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }

    @Override
    public synchronized long now() {
        return now;
    }

    @Override
    public void execute(Runnable task) {
        schedule(task, 0);
    }

    @Override
    public synchronized Cancellable schedule(Runnable task, long delayMs) {
        Task t = new Task(now + Math.max(0, delayMs), seq++, task);
        queue.add(t);
        return () -> t.cancelled = true;
    }

    @Override
    public void shutdown() {}

    /** Runs every task due at or before {@code time}, then sets the clock to {@code time}. */
    public void runUntil(long time) {
        while (true) {
            Task t;
            synchronized (this) {
                if (queue.isEmpty() || queue.peek().time > time) {
                    now = Math.max(now, time);
                    return;
                }
                t = queue.poll();
                now = Math.max(now, t.time);
            }
            if (!t.cancelled) t.runnable.run();
        }
    }

    public void advance(long millis) {
        runUntil(now() + millis);
    }

    public void runPending() {
        runUntil(now());
    }
}
