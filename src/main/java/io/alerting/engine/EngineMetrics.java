package io.alerting.engine;

import io.alerting.model.AlertState;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** Engine counters. Read with {@link #snapshot()}. */
public final class EngineMetrics {
    public final LongAdder eventsReceived = new LongAdder();
    public final LongAdder eventsDropped = new LongAdder();
    public final LongAdder eventsRouted = new LongAdder();
    public final LongAdder prepareErrors = new LongAdder();
    public final LongAdder evaluations = new LongAdder();
    public final LongAdder evaluationErrors = new LongAdder();
    public final LongAdder sinkErrors = new LongAdder();
    public final LongAdder ruleRefreshes = new LongAdder();
    public final LongAdder ruleRefreshFailures = new LongAdder();
    private final Map<AlertState, LongAdder> alerts = new EnumMap<>(AlertState.class);

    public EngineMetrics() {
        for (AlertState s : AlertState.values()) alerts.put(s, new LongAdder());
    }

    void alertEmitted(AlertState state) {
        alerts.get(state).increment();
    }

    public long alerts(AlertState state) {
        return alerts.get(state).sum();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("events_received", eventsReceived.sum());
        m.put("events_dropped", eventsDropped.sum());
        m.put("events_routed", eventsRouted.sum());
        m.put("prepare_errors", prepareErrors.sum());
        m.put("evaluations", evaluations.sum());
        m.put("evaluation_errors", evaluationErrors.sum());
        m.put("sink_errors", sinkErrors.sum());
        m.put("rule_refreshes", ruleRefreshes.sum());
        m.put("rule_refresh_failures", ruleRefreshFailures.sum());
        alerts.forEach((s, v) -> m.put("alerts_" + s.name().toLowerCase(), v.sum()));
        return m;
    }
}
