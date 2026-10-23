package io.alerting.engine;

import io.alerting.model.EntityKey;
import io.alerting.rules.RuleCatalog;
import io.alerting.rules.RuleCatalogHolder;
import io.alerting.rules.RuleEvaluator;
import io.alerting.rules.RuleProvider;
import io.alerting.sink.AlertSink;
import io.alerting.source.Source;
import io.alerting.source.SourceContext;
import io.alerting.stream.StreamDefinition;
import io.alerting.threshold.ChainedThresholdEvaluator;
import io.alerting.threshold.ThresholdEvaluator;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

/**
 * Generic streaming alert engine: any number of {@link Source}s feed any number of {@link StreamDefinition}s;
 * each stream keys its items, matches them to rules from a {@link RuleProvider}, runs the per-key alert state
 * machine and writes transitions to an {@link AlertSink}.
 *
 * <p>Events can also be pushed directly with {@link #submit}, which makes the engine embeddable behind any
 * transport (Kafka consumer, HTTP endpoint, message bus).
 */
public final class AlertEngine implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(AlertEngine.class.getName());

    private final EngineConfig config;
    private final EngineMetrics metrics = new EngineMetrics();
    private final RuleCatalogHolder rules;
    private final AlertSink sink;
    private final Map<String, List<StreamDefinition<?>>> streamsBySource;
    private final List<Source> sources;
    private final Scheduler control;
    private final Shard[] shards;
    private final List<Thread> sourceThreads = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private CountDownLatch sourcesDone = new CountDownLatch(0);

    private AlertEngine(Builder b) {
        this.config = b.config;
        this.sink = Objects.requireNonNull(b.sink, "sink");
        this.sources = List.copyOf(b.sources);
        this.rules = new RuleCatalogHolder(Objects.requireNonNull(b.ruleProvider, "ruleProvider"), metrics);

        Map<String, StreamDefinition<?>> byName = new LinkedHashMap<>();
        Map<String, List<StreamDefinition<?>>> bySource = new LinkedHashMap<>();
        for (StreamDefinition<?> s : b.streams) {
            if (byName.put(s.name(), s) != null) throw new IllegalArgumentException("duplicate stream name: " + s.name());
            bySource.computeIfAbsent(s.source(), k -> new ArrayList<>()).add(s);
        }
        if (byName.isEmpty()) throw new IllegalArgumentException("at least one stream is required");
        bySource.replaceAll((k, v) -> List.copyOf(v));
        this.streamsBySource = Collections.unmodifiableMap(bySource);

        Clock clock = b.clock;
        this.control = b.controlScheduler != null ? b.controlScheduler : new ExecutorScheduler("alert-rules", clock);
        IntFunction<Scheduler> shardSchedulers = b.shardSchedulers != null
                ? b.shardSchedulers
                : i -> new ExecutorScheduler("alert-shard-" + i, clock);

        EngineContext ctx = new EngineContext(config, rules, new RuleEvaluator(b.thresholds),
                new AlertIdGenerator(config.alertIdBytes()), sink, metrics, Collections.unmodifiableMap(byName));
        this.shards = new Shard[config.shards()];
        for (int i = 0; i < shards.length; i++) shards[i] = new Shard(shardSchedulers.apply(i), ctx);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Starts rule refresh and sources without waiting for rules (events are held until rules load). */
    public void start() throws InterruptedException {
        start(Duration.ZERO);
    }

    /** Starts rule refresh, waits up to {@code awaitRules} for the first catalog, then starts sources. */
    public void start(Duration awaitRules) throws InterruptedException {
        if (!running.compareAndSet(false, true)) throw new IllegalStateException("already started");
        control.execute(this::refreshLoop);
        if (!awaitRules.isZero() && !rules.awaitLoaded(awaitRules)) {
            LOG.log(System.Logger.Level.WARNING, "rules not loaded after " + awaitRules + "; starting sources anyway (evaluation is held until rules load)");
        }
        sourcesDone = new CountDownLatch(sources.size());
        for (Source source : sources) {
            Thread t = new Thread(() -> runSource(source), "source-" + source.name());
            t.setDaemon(true);
            sourceThreads.add(t);
            t.start();
        }
    }

    private void refreshLoop() {
        if (!running.get()) return;
        rules.refresh();
        control.schedule(this::refreshLoop, config.ruleRefreshInterval().toMillis());
    }

    private void runSource(Source source) {
        SourceContext ctx = new SourceContext() {
            @Override
            public void emit(Object record, Long timestamp) {
                submit(source.name(), record, timestamp);
            }

            @Override
            public void emitTo(String sourceName, Object record, Long timestamp) {
                submit(sourceName, record, timestamp);
            }

            @Override
            public boolean isRunning() {
                return running.get();
            }
        };
        try {
            LOG.log(System.Logger.Level.INFO, "source " + source.name() + " started");
            source.run(ctx);
            LOG.log(System.Logger.Level.INFO, "source " + source.name() + " finished");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.ERROR, "source " + source.name() + " failed", e);
        } finally {
            sourcesDone.countDown();
        }
    }

    public void submit(String sourceName, Object record) {
        submit(sourceName, record, null);
    }

    /**
     * Routes a raw record to every stream bound to {@code sourceName}. Thread-safe; blocks when the target
     * shard has {@code maxPendingPerShard} queued events.
     *
     * @param sourceTimestamp fallback event time (e.g. the Kafka record timestamp), may be null
     */
    public void submit(String sourceName, Object record, Long sourceTimestamp) {
        metrics.eventsReceived.increment();
        List<StreamDefinition<?>> targets = streamsBySource.get(sourceName);
        if (record == null || targets == null) {
            metrics.eventsDropped.increment();
            LOG.log(System.Logger.Level.DEBUG, () -> "dropping record from " + sourceName + (record == null ? ": null record" : ": no stream reads this source"));
            return;
        }
        for (StreamDefinition<?> stream : targets) route(stream, record, sourceTimestamp);
    }

    private <T> void route(StreamDefinition<T> stream, Object record, Long sourceTimestamp) {
        List<T> items;
        try {
            items = stream.prepare(record);
        } catch (RuntimeException e) {
            metrics.prepareErrors.increment();
            LOG.log(System.Logger.Level.WARNING, "stream " + stream.name() + ": cannot prepare record: " + e);
            return;
        }
        for (T item : items) {
            StateKey sk;
            long ts;
            try {
                ts = eventTime(stream.timestamp(item), sourceTimestamp);
                EntityKey key = stream.key(item);
                if (key == null) {
                    metrics.eventsDropped.increment();
                    continue;
                }
                if (key.date() == 0) key = key.withDate(EndOfDay.dateOf(ts, config.zone()));
                EndOfDay.toLocalDate(key.date()); // validates
                sk = new StateKey(stream.name(), key);
            } catch (RuntimeException e) {
                metrics.prepareErrors.increment();
                LOG.log(System.Logger.Level.WARNING, "stream " + stream.name() + ": cannot key item: " + e);
                continue;
            }
            Shard shard = shards[Math.floorMod(sk.hashCode(), shards.length)];
            shard.permits.acquireUninterruptibly();
            metrics.eventsRouted.increment();
            long eventTime = ts;
            shard.scheduler.execute(() -> {
                try {
                    shard.onEvent(sk, item, eventTime);
                } finally {
                    shard.permits.release();
                }
            });
        }
    }

    private long eventTime(Long extracted, Long sourceTimestamp) {
        if (extracted != null && extracted > 0) return extracted;
        if (sourceTimestamp != null && sourceTimestamp > 0) return sourceTimestamp;
        return control.now();
    }

    /** Forces a rule refresh now (also runs every {@code ruleRefreshInterval}). */
    public boolean refreshRules() {
        return rules.refresh();
    }

    public RuleCatalog rules() {
        return rules.current();
    }

    public EngineMetrics metrics() {
        return metrics;
    }

    public EngineConfig config() {
        return config;
    }

    /** Number of (stream, key) states held. Exact only when shards are idle. */
    public int stateCount() {
        int n = 0;
        for (Shard s : shards) n += s.size();
        return n;
    }

    /** Waits for all finite sources to finish. Null timeout waits forever. */
    public boolean awaitSources(Duration timeout) throws InterruptedException {
        if (timeout == null) {
            sourcesDone.await();
            return true;
        }
        return sourcesDone.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Waits until everything submitted so far has been evaluated: queued events run and the throttle timers
     * they armed fire. Pending end-of-day timers are not waited for.
     */
    public boolean drain(Duration timeout) throws InterruptedException {
        Set<Scheduler> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Shard s : shards) distinct.add(s.scheduler);
        CountDownLatch latch = new CountDownLatch(distinct.size());
        long delay = config.outputInterval().toMillis() + 50;
        for (Scheduler s : distinct) s.schedule(latch::countDown, delay);
        return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        running.set(false);
        for (Source s : sources) {
            try {
                s.stop();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "stopping source " + s.name() + " failed", e);
            }
        }
        sourceThreads.forEach(Thread::interrupt);
        control.shutdown();
        Set<Scheduler> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Shard s : shards) distinct.add(s.scheduler);
        distinct.forEach(Scheduler::shutdown);
        try {
            sink.close();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "closing sink failed", e);
        }
    }

    public static final class Builder {
        private EngineConfig config = EngineConfig.defaults();
        private RuleProvider ruleProvider;
        private AlertSink sink;
        private final List<StreamDefinition<?>> streams = new ArrayList<>();
        private final List<Source> sources = new ArrayList<>();
        private ThresholdEvaluator thresholds = ChainedThresholdEvaluator.standard();
        private Clock clock = Clock.systemUTC();
        private Scheduler controlScheduler;
        private IntFunction<Scheduler> shardSchedulers;

        public Builder config(EngineConfig v) { config = v; return this; }
        public Builder ruleProvider(RuleProvider v) { ruleProvider = v; return this; }
        public Builder sink(AlertSink v) { sink = v; return this; }
        public Builder stream(StreamDefinition<?> v) { streams.add(v); return this; }
        public Builder streams(List<? extends StreamDefinition<?>> v) { streams.addAll(v); return this; }
        public Builder source(Source v) { sources.add(v); return this; }
        public Builder sources(List<? extends Source> v) { sources.addAll(v); return this; }
        /** Replaces the threshold evaluator chain (e.g. {@code ChainedThresholdEvaluator.standard(myCustomType)}). */
        public Builder thresholds(ThresholdEvaluator v) { thresholds = v; return this; }
        public Builder clock(Clock v) { clock = v; return this; }

        /** Runs everything on one scheduler, e.g. a {@link ManualScheduler} for deterministic tests and replays. */
        public Builder scheduler(Scheduler v) {
            controlScheduler = v;
            shardSchedulers = i -> v;
            return this;
        }

        public AlertEngine build() {
            return new AlertEngine(this);
        }
    }
}
