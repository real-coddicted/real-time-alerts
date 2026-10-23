package io.alerting.source;

/** Emits a fixed collection of records, optionally spaced out. Handy for demos and tests. */
public final class IterableSource implements Source {
    private final String name;
    private final Iterable<?> records;
    private final long delayMs;

    public IterableSource(String name, Iterable<?> records, long delayMs) {
        this.name = name;
        this.records = records;
        this.delayMs = delayMs;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void run(SourceContext ctx) throws InterruptedException {
        for (Object r : records) {
            if (!ctx.isRunning()) return;
            ctx.emit(r, null);
            if (delayMs > 0) Thread.sleep(delayMs);
        }
    }
}
