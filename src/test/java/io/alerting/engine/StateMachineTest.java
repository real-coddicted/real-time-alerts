package io.alerting.engine;

import io.alerting.model.Alert;
import io.alerting.model.AlertState;
import io.alerting.model.EntityKey;
import io.alerting.model.Metric;
import io.alerting.model.Rule;
import io.alerting.rules.RuleProvider;
import io.alerting.sink.InMemorySink;
import io.alerting.stream.StreamDefinition;
import io.alerting.testing.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.alerting.rules.RuleEvaluatorTest.rule;
import static io.alerting.testing.Assert.assertEquals;
import static io.alerting.testing.Assert.assertTrue;

/** Per-key state machine behavior (spec 5.x and the S1..S6 scenarios), driven on virtual time. */
public class StateMachineTest {
    static final long START = Instant.parse("2024-09-18T14:00:00Z").toEpochMilli();
    static final long INTERVAL = 100;

    record Tick(String unit, String pnl, long ts) {}

    static final class ToggleProvider implements RuleProvider {
        volatile List<Rule> rules;
        volatile boolean failing;

        ToggleProvider(List<Rule> rules) {
            this.rules = rules;
        }

        @Override
        public List<Rule> fetchRules() throws IOException {
            if (failing) throw new IOException("rules API down");
            return rules;
        }
    }

    static final class Harness {
        final ManualScheduler scheduler = new ManualScheduler(START);
        final InMemorySink sink = new InMemorySink();
        final ToggleProvider rules;
        final AlertEngine engine;

        Harness(EngineConfig.Builder config, Rule... rules) throws Exception {
            this(config, false, rules);
        }

        Harness(EngineConfig.Builder config, boolean rulesInitiallyFailing, Rule... rules) throws Exception {
            this.rules = new ToggleProvider(List.of(rules));
            this.rules.failing = rulesInitiallyFailing;
            StreamDefinition<Tick> stream = StreamDefinition.builder("ticks", "src")
                    .as(Tick.class)
                    .key(t -> EntityKey.of("UNIT", 20240918, Map.of("unit_id", t.unit())))
                    .timestamp(Tick::ts)
                    .subject("PNL", t -> Metric.of("pnl", t.pnl(), "base", "100"))
                    .build();
            engine = AlertEngine.builder()
                    .config(config.outputInterval(Duration.ofMillis(INTERVAL)).shards(2).build())
                    .ruleProvider(this.rules)
                    .sink(sink)
                    .stream(stream)
                    .scheduler(scheduler)
                    .build();
            engine.start();
            scheduler.runPending();
        }

        /** Sends one event and lets its evaluation run; returns the alerts it produced. */
        List<Alert> step(String pnl, long eventTime) {
            int before = sink.size();
            engine.submit("src", new Tick("u1", pnl, eventTime));
            scheduler.advance(INTERVAL + 1);
            return sink.alerts().subList(before, sink.size());
        }
    }

    static EngineConfig.Builder cfg() {
        return EngineConfig.builder().keepTicking(true);
    }

    static Rule fireBelow(String id, String threshold, String extra) {
        return rule("{\"id\":\"" + id + "\",\"subject\":\"PNL\",\"scope\":\"UNIT\""
                + ",\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"" + threshold + "\",\"expiryMs\":14400000}"
                + (extra.isEmpty() ? "" : "," + extra) + "}");
    }

    static final String RESOLVE = "\"resolve\":{\"type\":\"RATIO\",\"operator\":\"GT\",\"value\":\"-0.20\",\"minTimeToResolveMs\":5000}";

    private static List<AlertState> states(List<Alert> alerts) {
        return alerts.stream().map(Alert::state).toList();
    }

    @Test
    void s1ThrottleEvaluatesLatestEventWithFirstEventTime() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", ""));
        h.engine.submit("src", new Tick("u1", "-30", START + 1));
        h.engine.submit("src", new Tick("u1", "-50", START + 2));
        h.scheduler.advance(INTERVAL + 1);
        List<Alert> out = h.sink.alerts();
        assertEquals(List.of(AlertState.FIRED), states(out));
        assertEquals("-50", out.get(0).value().subjectValue().value());
        assertEquals(START + 1, out.get(0).createdAt());
        assertEquals(1L, h.engine.metrics().evaluations.sum());
    }

    @Test
    void s2RuleExpiryResolvesWithFireTimeValue() throws Exception {
        Rule r = rule("{\"id\":\"R\",\"subject\":\"PNL\",\"scope\":\"UNIT\",\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"-0.20\",\"expiryMs\":1000}}");
        Harness h = new Harness(cfg(), r);
        Alert fired = h.step("-30", START).get(0);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(h.step("-10", START + 999)), "before expiry, keepTicking");
        List<Alert> out = h.step("-11", START + 1000);
        assertEquals(List.of(AlertState.RESOLVED_EXPIRED), states(out));
        assertEquals(fired.alertId(), out.get(0).alertId());
        assertEquals("-30", out.get(0).value().subjectValue().value());
        assertEquals(0, h.engine.stateCount());
    }

    @Test
    void s3MinTimeToResolve() throws Exception {
        Harness ticking = new Harness(cfg(), fireBelow("R", "-0.20", RESOLVE));
        ticking.step("-30", START);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(ticking.step("-10", START + 1000)));
        assertEquals(List.of(AlertState.RESOLVED_AUTOMATICALLY), states(ticking.step("-11", START + 5000)));

        Harness quiet = new Harness(cfg().keepTicking(false), fireBelow("R", "-0.20", RESOLVE));
        quiet.step("-30", START);
        assertEquals(List.of(), states(quiet.step("-10", START + 1000)));
    }

    @Test
    void s4DedupeAgainstFireValue() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", ""));
        h.step("-30", START);
        assertEquals(List.of(), states(h.step("-30.00", START + 10)));
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(h.step("-40", START + 20)));
        // Default: stored value stays the fire value, so a stable -40 keeps re-emitting with keepTicking.
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(h.step("-40", START + 30)));
    }

    @Test
    void dedupeAgainstLastEmittedOption() throws Exception {
        Harness h = new Harness(cfg().dedupeAgainstLastEmitted(true), fireBelow("R", "-0.20", ""));
        h.step("-30", START);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(h.step("-40", START + 20)));
        assertEquals(List.of(), states(h.step("-40", START + 30)));
        assertEquals("-30", h.step("-45", START + 40).get(0).value().breachValue().value(), "breach still the first value");
    }

    @Test
    void s5EndOfDayExpiresAllAlertsAndClearsKey() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("A", "-0.20", ""), fireBelow("B", "-0.25", ""));
        assertEquals(2, h.step("-30", START).size());
        h.scheduler.advance(Duration.ofHours(16).toMillis());
        List<Alert> out = h.sink.alerts().subList(2, h.sink.size());
        assertEquals(List.of(AlertState.RESOLVED_EXPIRED, AlertState.RESOLVED_EXPIRED), states(out));
        assertEquals(Instant.parse("2024-09-19T03:59:59.999Z").toEpochMilli(), out.get(0).updatedAt(), "end of 2024-09-18 in New York");
        assertEquals(0, h.engine.stateCount());
        assertEquals(List.of(AlertState.FIRED, AlertState.FIRED), states(h.step("-30", START + 1)), "late events start a fresh cycle");
    }

    @Test
    void s6RulesOutageKeepsLastGoodCatalog() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", RESOLVE.replace("5000", "0")));
        h.step("-30", START);
        h.rules.failing = true;
        assertTrue(!h.engine.refreshRules(), "refresh fails");
        assertEquals(1L, h.engine.metrics().ruleRefreshFailures.sum());
        assertEquals(List.of(AlertState.RESOLVED_AUTOMATICALLY), states(h.step("-10", START + 10)), "still resolves");
    }

    @Test
    void eventsAreHeldUntilRulesLoad() throws Exception {
        Harness h = new Harness(cfg(), true, fireBelow("R", "-0.20", ""));
        assertEquals(List.of(), states(h.step("-30", START)));
        h.rules.failing = false;
        h.engine.refreshRules();
        h.scheduler.advance(INTERVAL + 1);
        assertEquals(List.of(AlertState.FIRED), states(h.sink.alerts()));
    }

    @Test
    void ruleRemovalResolvesActiveAlert() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", ""));
        h.step("-30", START);
        h.rules.rules = List.of();
        h.engine.refreshRules();
        assertEquals(List.of(AlertState.RESOLVED_RULE_REMOVED), states(h.step("-40", START + 10)));
        assertEquals(0, h.engine.stateCount());

        Harness legacy = new Harness(cfg().resolveOnRuleRemoval(false), fireBelow("R", "-0.20", ""));
        legacy.step("-30", START);
        legacy.rules.rules = List.of();
        legacy.engine.refreshRules();
        assertEquals(List.of(), states(legacy.step("-40", START + 10)));
        assertEquals(1, legacy.engine.stateCount(), "orphaned until end of day");
    }

    @Test
    void editingARuleKeepsTheAlert() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", ""));
        Alert fired = h.step("-30", START).get(0);
        Rule v2 = rule("{\"id\":\"R\",\"version\":2,\"subject\":\"PNL\",\"scope\":\"UNIT\",\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"-0.25\"}}");
        h.rules.rules = List.of(v2);
        h.engine.refreshRules();
        List<Alert> out = h.step("-40", START + 10);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(out));
        assertEquals(fired.alertId(), out.get(0).alertId());
        assertEquals(2L, out.get(0).rule().version());
    }

    @Test
    void poisonEventIsSkipped() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", ""));
        assertEquals(List.of(), states(h.step("not-a-number", START)));
        assertEquals(1L, h.engine.metrics().evaluationErrors.sum());
        assertEquals(List.of(AlertState.FIRED), states(h.step("-30", START + 1)));
    }

    @Test
    void keysWithoutAlertsHoldNoState() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", ""));
        h.step("-10", START);
        assertEquals(0, h.engine.stateCount());
    }

    @Test
    void processingTimeBasis() throws Exception {
        Harness h = new Harness(cfg().timeBasis(TimeBasis.PROCESSING), fireBelow("R", "-0.20", ""));
        long submittedAt = h.scheduler.now();
        Alert a = h.step("-30", 42L).get(0);
        assertEquals(submittedAt + INTERVAL, a.createdAt());
    }

    @Test
    void nullAndUnroutedRecordsAreDropped() throws Exception {
        Harness h = new Harness(cfg(), fireBelow("R", "-0.20", ""));
        h.engine.submit("src", null);
        h.engine.submit("nobody-reads-this", new Tick("u1", "-30", START));
        h.engine.submit("src", "wrong type");
        h.scheduler.advance(INTERVAL + 1);
        assertEquals(0, h.sink.size());
        assertEquals(2L, h.engine.metrics().eventsDropped.sum());
    }
}
