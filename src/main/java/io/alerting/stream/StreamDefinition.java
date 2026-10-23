package io.alerting.stream;

import io.alerting.model.EntityKey;
import io.alerting.model.Metric;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * One alerting branch: which source it reads, how raw records become keyed items (filter / map / flatMap),
 * how an item is keyed, and which rule subjects it can evaluate. Each stream has its own alert state, so
 * any number of streams may read the same source.
 *
 * <pre>{@code
 * StreamDefinition.builder("pnl-position", "tick-pnl")
 *     .as(PnlResult.class)
 *     .filter(p -> p.hasInstrumentId())
 *     .key(p -> EntityKey.of("POSITION", p.getDate(), Map.of("unit_id", p.getUnitId(), ...)))
 *     .timestamp(PnlResult::getTimestamp)
 *     .subject("TOTAL_PNL", p -> Metric.of("total_pnl", p.getTotalPnl(), "sod_equity", p.getSodEquity()))
 *     .build();
 * }</pre>
 */
public final class StreamDefinition<T> {
    private final String name;
    private final String source;
    private final Function<Object, Stream<T>> pipeline;
    private final Function<? super T, EntityKey> key;
    private final Function<? super T, Long> timestamp;
    private final Map<String, Function<? super T, Metric>> subjects;

    private StreamDefinition(Builder<T> b) {
        this.name = b.name;
        this.source = b.source;
        this.pipeline = b.pipeline;
        this.key = b.key;
        this.timestamp = b.timestamp;
        this.subjects = Map.copyOf(b.subjects);
    }

    public static Builder<Object> builder(String name, String source) {
        return new Builder<>(name, source, Stream::of);
    }

    public String name() {
        return name;
    }

    public String source() {
        return source;
    }

    public Set<String> subjects() {
        return subjects.keySet();
    }

    /** Runs the filter/map/flatMap pipeline on a raw source record. */
    public List<T> prepare(Object record) {
        return pipeline.apply(record).toList();
    }

    public EntityKey key(T item) {
        return key.apply(item);
    }

    /** Event time in epoch ms, or null to fall back to the source/ingest timestamp. */
    public Long timestamp(T item) {
        return timestamp == null ? null : timestamp.apply(item);
    }

    @SuppressWarnings("unchecked")
    public Metric metric(String subject, Object item) {
        Function<? super T, Metric> f = subjects.get(subject);
        if (f == null) throw new IllegalArgumentException("stream " + name + " does not support subject " + subject);
        return f.apply((T) item);
    }

    public static final class Builder<T> {
        private final String name;
        private final String source;
        private final Function<Object, Stream<T>> pipeline;
        private Function<? super T, EntityKey> key;
        private Function<? super T, Long> timestamp;
        private final Map<String, Function<? super T, Metric>> subjects = new LinkedHashMap<>();

        private Builder(String name, String source, Function<Object, Stream<T>> pipeline) {
            this.name = Objects.requireNonNull(name, "name");
            this.source = Objects.requireNonNull(source, "source");
            this.pipeline = pipeline;
        }

        private <R> Builder<R> next(Function<Object, Stream<R>> p) {
            if (key != null || timestamp != null || !subjects.isEmpty()) {
                throw new IllegalStateException("pipeline steps must come before key/timestamp/subject");
            }
            return new Builder<>(name, source, p);
        }

        /** Keeps only records of {@code type} and narrows the builder to it. */
        public <R> Builder<R> as(Class<R> type) {
            return next(pipeline.andThen(s -> s.filter(type::isInstance).map(type::cast)));
        }

        public Builder<T> filter(Predicate<? super T> predicate) {
            return next(pipeline.andThen(s -> s.filter(predicate)));
        }

        public <R> Builder<R> map(Function<? super T, ? extends R> f) {
            return next(pipeline.andThen(s -> s.<R>map(f)));
        }

        public <R> Builder<R> flatMap(Function<? super T, ? extends Collection<? extends R>> f) {
            return next(pipeline.andThen(s -> s.<R>flatMap(t -> f.apply(t).stream())));
        }

        public Builder<T> key(Function<? super T, EntityKey> keyFn) {
            this.key = keyFn;
            return this;
        }

        public Builder<T> timestamp(Function<? super T, Long> timestampFn) {
            this.timestamp = timestampFn;
            return this;
        }

        public Builder<T> subject(String subject, Function<? super T, Metric> extractor) {
            subjects.put(subject, extractor);
            return this;
        }

        public StreamDefinition<T> build() {
            if (key == null) throw new IllegalStateException("stream " + name + ": key is required");
            if (subjects.isEmpty()) throw new IllegalStateException("stream " + name + ": at least one subject is required");
            return new StreamDefinition<>(this);
        }
    }
}
