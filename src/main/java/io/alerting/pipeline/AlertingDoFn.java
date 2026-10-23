package io.alerting.pipeline;

import io.alerting.config.CompiledStream;
import io.alerting.core.AlertStateMachine;
import io.alerting.core.EndOfDay;
import io.alerting.core.EngineConfig;
import io.alerting.core.TimeBasis;
import io.alerting.json.Json;
import io.alerting.model.Alert;
import io.alerting.model.AlertState;
import io.alerting.model.EntityKey;
import io.alerting.model.Rule;
import io.alerting.rules.RuleCatalog;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The per-key alert state machine as a stateful DoFn (spec section 5). One instance per stream; Beam
 * shuffles by {@link EntityKey}, so all state below is per (stream, key).
 *
 * <ul>
 *   <li>processElement conflates a burst: it keeps the latest event and, on the first event, arms the
 *       throttle timer and snapshots the rules matched to the key from the rules side input (side inputs
 *       are not readable in timers).</li>
 *   <li>The output timer evaluates the latest event against that snapshot via {@link AlertStateMachine}.</li>
 *   <li>The expiry timer fires at end of business day and expires everything on the key.</li>
 * </ul>
 * A key with no active alert keeps no state after its evaluation, so state stays bounded (spec K9).
 */
public final class AlertingDoFn extends DoFn<KV<EntityKey, Event>, KV<String, String>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertingDoFn.class);

    private static final String ALERTS = "alerts";
    private static final String LATEST = "latestEvent";
    private static final String BURST_TS = "burstTimestamp";
    private static final String ARMED = "outputArmed";
    private static final String RULES = "matchedRules";
    private static final String KEY = "key";
    private static final String EXPIRY_ARMED = "expiryArmed";
    private static final String OUTPUT_TIMER = "outputTimer";
    private static final String EXPIRY_TIMER = "expiryTimer";

    /** Active alert per rule id. */
    @StateId(ALERTS)
    private final StateSpec<MapState<String, Alert>> alertsSpec = StateSpecs.map(StringUtf8Coder.of(), Coders.AlertCoder.of());

    /** Latest event of the current burst (JSON); only this one is evaluated. */
    @StateId(LATEST)
    private final StateSpec<ValueState<String>> latestSpec = StateSpecs.value(StringUtf8Coder.of());

    /** Event time of the first event of the current burst. */
    @StateId(BURST_TS)
    private final StateSpec<ValueState<Long>> burstSpec = StateSpecs.value(VarLongCoder.of());

    @StateId(ARMED)
    private final StateSpec<ValueState<Boolean>> armedSpec = StateSpecs.value(BooleanCoder.of());

    /** Rules matched to the key when the burst started. */
    @StateId(RULES)
    private final StateSpec<ValueState<List<Rule>>> rulesSpec = StateSpecs.value(ListCoder.of(Coders.RuleCoder.of()));

    @StateId(KEY)
    private final StateSpec<ValueState<EntityKey>> keySpec = StateSpecs.value(Coders.EntityKeyCoder.of());

    @StateId(EXPIRY_ARMED)
    private final StateSpec<ValueState<Boolean>> expiryArmedSpec = StateSpecs.value(BooleanCoder.of());

    @TimerId(OUTPUT_TIMER)
    private final TimerSpec outputTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @TimerId(EXPIRY_TIMER)
    private final TimerSpec expiryTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private final String streamName;
    private final String streamSpecJson;
    private final EngineConfig config;
    private final PCollectionView<RuleCatalog> rulesView;

    private transient CompiledStream stream;
    private transient AlertStateMachine machine;
    private transient Counter evaluations;
    private transient Counter evaluationErrors;
    private transient Map<AlertState, Counter> emitted;

    public AlertingDoFn(Map<String, Object> streamSpec, EngineConfig config, PCollectionView<RuleCatalog> rulesView) {
        this.streamSpecJson = Json.write(streamSpec);
        this.streamName = CompiledStream.compile(streamSpec).name();
        this.config = config;
        this.rulesView = rulesView;
    }

    @Setup
    public void setup() {
        stream = CompiledStream.compile(Json.parseObject(streamSpecJson));
        machine = new AlertStateMachine(config);
        String ns = "alerting.stream." + streamName;
        evaluations = Metrics.counter(ns, "evaluations");
        evaluationErrors = Metrics.counter(ns, "evaluation_errors");
        emitted = new EnumMap<>(AlertState.class);
        for (AlertState s : AlertState.values()) emitted.put(s, Metrics.counter(ns, "alerts_" + s.name().toLowerCase()));
    }

    @ProcessElement
    public void process(ProcessContext c,
                        @StateId(LATEST) ValueState<String> latest,
                        @StateId(BURST_TS) ValueState<Long> burst,
                        @StateId(ARMED) ValueState<Boolean> armed,
                        @StateId(RULES) ValueState<List<Rule>> rules,
                        @StateId(KEY) ValueState<EntityKey> keyState,
                        @TimerId(OUTPUT_TIMER) Timer outputTimer) {
        KV<EntityKey, Event> element = c.element();
        latest.write(element.getValue().json());
        if (!Boolean.TRUE.equals(armed.read())) {
            armed.write(true);
            burst.write(element.getValue().timestamp());
            keyState.write(element.getKey());
            outputTimer.offset(Duration.millis(config.outputInterval().toMillis())).setRelative();
            RuleCatalog catalog = c.sideInput(rulesView);
            rules.write(catalog.match(element.getKey(), stream.subjects()));
        }
    }

    @OnTimer(OUTPUT_TIMER)
    public void onOutputTimer(OutputReceiver<KV<String, String>> out,
                              @StateId(ALERTS) MapState<String, Alert> alerts,
                              @StateId(LATEST) ValueState<String> latest,
                              @StateId(BURST_TS) ValueState<Long> burst,
                              @StateId(ARMED) ValueState<Boolean> armed,
                              @StateId(RULES) ValueState<List<Rule>> rules,
                              @StateId(KEY) ValueState<EntityKey> keyState,
                              @StateId(EXPIRY_ARMED) ValueState<Boolean> expiryArmed,
                              @TimerId(EXPIRY_TIMER) Timer expiryTimer) {
        if (!Boolean.TRUE.equals(armed.read())) return;
        String json = latest.read();
        Long burstTs = burst.read();
        EntityKey key = keyState.read();
        List<Rule> matched = rules.read();
        armed.clear();
        latest.clear();
        burst.clear();
        if (json == null || key == null || burstTs == null) return;

        Map<String, Alert> active = readAll(alerts);
        Set<String> before = new HashSet<>(active.keySet());
        long wall = System.currentTimeMillis();
        long now = config.timeBasis() == TimeBasis.EVENT ? burstTs : wall;

        Map<String, Object> item = Json.parseObject(json);
        AlertStateMachine.Outcome outcome = machine.evaluate(streamName, key, matched == null ? List.of() : matched,
                active, subject -> stream.metric(subject, item), now, wall);
        evaluations.inc();
        if (outcome.errors() > 0) evaluationErrors.inc(outcome.errors());

        for (String id : before) {
            if (!active.containsKey(id)) alerts.remove(id);
        }
        active.forEach(alerts::put);
        emit(out, outcome.emitted());

        if (active.isEmpty()) {
            alerts.clear();
            rules.clear();
            keyState.clear();
            expiryArmed.clear();
            expiryTimer.clear();
        } else if (outcome.newlyFired() && !Boolean.TRUE.equals(expiryArmed.read())) {
            expiryTimer.offset(Duration.millis(EndOfDay.millisUntilEndOfDay(key.date(), wall, config.zone()))).setRelative();
            expiryArmed.write(true);
        }
    }

    @OnTimer(EXPIRY_TIMER)
    public void onExpiryTimer(OutputReceiver<KV<String, String>> out,
                              @StateId(ALERTS) MapState<String, Alert> alerts,
                              @StateId(LATEST) ValueState<String> latest,
                              @StateId(BURST_TS) ValueState<Long> burst,
                              @StateId(ARMED) ValueState<Boolean> armed,
                              @StateId(RULES) ValueState<List<Rule>> rules,
                              @StateId(KEY) ValueState<EntityKey> keyState,
                              @StateId(EXPIRY_ARMED) ValueState<Boolean> expiryArmed,
                              @TimerId(OUTPUT_TIMER) Timer outputTimer) {
        if (!Boolean.TRUE.equals(expiryArmed.read())) return;
        emit(out, machine.expireAll(readAll(alerts), System.currentTimeMillis()));
        alerts.clear();
        latest.clear();
        burst.clear();
        armed.clear();
        rules.clear();
        keyState.clear();
        expiryArmed.clear();
        outputTimer.clear();
    }

    private void emit(OutputReceiver<KV<String, String>> out, List<Alert> alerts) {
        for (Alert a : alerts) {
            out.output(KV.of(Json.write(a.keyJson()), Json.write(a.toJson())));
            emitted.get(a.state()).inc();
            LOG.debug("alert {} {} rule={} key={}", a.alertId(), a.state(), a.rule().id(), a.key().canonical());
        }
    }

    private static Map<String, Alert> readAll(MapState<String, Alert> alerts) {
        Map<String, Alert> m = new LinkedHashMap<>();
        Iterable<Map.Entry<String, Alert>> entries = alerts.entries().read();
        if (entries != null) {
            for (Map.Entry<String, Alert> e : entries) m.put(e.getKey(), e.getValue());
        }
        return m;
    }
}
