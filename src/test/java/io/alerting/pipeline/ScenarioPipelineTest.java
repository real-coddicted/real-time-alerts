package io.alerting.pipeline;

import io.alerting.config.PipelineConfig;
import io.alerting.json.Json;
import io.alerting.json.JsonAccess;
import io.alerting.rules.RuleCatalog;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replays the example scenarios through the real Beam graph (Prepare → stateful AlertingDoFn → Flatten) on
 * the DirectRunner, advancing processing time past the throttle after every event. Finance (spec 12.4,
 * E1..E17) and security run on the same code; only the config differs.
 */
class ScenarioPipelineTest {
    private static final long INTERVAL = 10;

    private static List<Map<String, Object>> replay(Pipeline p, PipelineConfig config, List<PipelineTestSupport.Step> steps,
                                                   SerializableFunction<Iterable<KV<String, String>>, Void> check) throws Exception {
        TestStream.Builder<KV<String, Event>> b = PipelineTestSupport.stream().advanceProcessingTime(Duration.millis(1));
        for (PipelineTestSupport.Step s : steps) {
            b = b.addElements(KV.of(s.source, new Event(s.recordJson, 0L))).advanceProcessingTime(Duration.millis(INTERVAL + 1));
        }
        PCollection<KV<String, Event>> input = p.apply(b.advanceWatermarkToInfinity());
        PCollectionView<RuleCatalog> rules = PipelineTestSupport.staticRules(p, config);
        PCollection<KV<String, String>> alerts = AlertingPipeline.alerts(config, PipelineTestSupport.sources(config, input), rules);
        PAssert.that(alerts).satisfies(check);
        p.run().waitUntilFinish();
        return List.of();
    }

    private static List<String> expectedMultiset(List<PipelineTestSupport.Step> steps) {
        List<String> all = new ArrayList<>();
        steps.forEach(s -> all.addAll(s.expectedNew));
        Collections.sort(all);
        return all;
    }

    static List<Map<String, Object>> parse(Iterable<KV<String, String>> records) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (KV<String, String> kv : records) {
            Map<String, Object> alert = Json.parseObject(kv.getValue());
            Map<String, Object> key = Json.parseObject(kv.getKey());
            assertEquals(alert.get("alert_id"), key.get("alert_id"), "sink key carries the alert id");
            out.add(alert);
        }
        return out;
    }

    static String stateRule(Map<String, Object> a) {
        return a.get("state") + " " + JsonAccess.requireMap(a, "rule").get("id");
    }

    static String attr(Map<String, Object> a, String name) {
        return String.valueOf(JsonAccess.requireMap(JsonAccess.requireMap(a, "rule_object"), "attributes").get(name));
    }

    static String value(Map<String, Object> a, String field) {
        return String.valueOf(JsonAccess.requireMap(JsonAccess.requireMap(a, "value"), field).get("value"));
    }

    @Test
    void riskScenarioE1toE17() throws Exception {
        PipelineConfig config = PipelineTestSupport.load("examples/risk/pipeline.json", Long.toString(INTERVAL));
        List<PipelineTestSupport.Step> steps = PipelineTestSupport.scenario("examples/risk/scenario.jsonl");
        List<String> expected = expectedMultiset(steps);
        assertEquals(25, expected.size());

        replay(PipelineTestSupport.pipeline(), config, steps, records -> {
            List<Map<String, Object>> alerts = parse(records);
            List<String> actual = new ArrayList<>();
            alerts.forEach(a -> actual.add(stateRule(a)));
            Collections.sort(actual);
            assertEquals(expected, actual, "alert transitions (state, rule)");

            // E6 -> E7 -> E8 is one lifecycle; E12 re-fires R18 on the same key at the same event time with a new id.
            Map<String, Set<String>> r18 = new TreeMap<>();
            for (Map<String, Object> a : alerts) {
                if (stateRule(a).endsWith(" R18") && attr(a, "unit_id").equals("100000")) {
                    r18.computeIfAbsent(a.get("alert_id").toString(), k -> new TreeSet<>()).add(a.get("state").toString());
                    assertEquals("1724439983468", a.get("created_at").toString(), "event-time stamping");
                }
            }
            assertEquals(Set.of(Set.of("FIRED", "FIRED_UPDATED", "RESOLVED_AUTOMATICALLY"), Set.of("FIRED", "FIRED_UPDATED")),
                    new java.util.HashSet<>(r18.values()));
            for (Map<String, Object> a : alerts) {
                if (a.get("state").equals("RESOLVED_AUTOMATICALLY")) {
                    assertEquals("-300.91", value(a, "breach_value"), "breach carried from fire");
                    assertEquals("-30.91", value(a, "subject_value"));
                    assertEquals("1724459983468", a.get("updated_at").toString());
                }
                if (stateRule(a).equals("FIRED R13") && attr(a, "unit_id").equals("1221")) {
                    assertEquals("-1.000000000000000000", JsonAccess.requireMap(a, "value").get("metric_value"));
                }
            }
            Set<String> issuers = new TreeSet<>();
            for (Map<String, Object> a : alerts) {
                if (stateRule(a).equals("FIRED R25")) {
                    issuers.add(attr(a, "underlying_instrument_id"));
                    assertEquals("CST", attr(a, "netting_id"), "netting carried onto liquidation keys");
                }
            }
            assertEquals(Set.of("227", "873"), issuers);
            return null;
        });
    }

    @Test
    void securityScenario() throws Exception {
        PipelineConfig config = PipelineTestSupport.load("examples/security/pipeline.json", Long.toString(INTERVAL));
        List<PipelineTestSupport.Step> steps = PipelineTestSupport.scenario("examples/security/scenario.jsonl");
        List<String> expected = expectedMultiset(steps);
        long fired = Instant.parse("2026-10-23T14:00:00Z").toEpochMilli();
        long resolved = Instant.parse("2026-10-23T14:10:00Z").toEpochMilli();

        replay(PipelineTestSupport.pipeline(), config, steps, records -> {
            List<Map<String, Object>> alerts = parse(records);
            List<String> actual = new ArrayList<>();
            alerts.forEach(a -> actual.add(stateRule(a)));
            Collections.sort(actual);
            assertEquals(expected, actual, "alert transitions (state, rule)");
            for (Map<String, Object> a : alerts) {
                if (stateRule(a).equals("RESOLVED_AUTOMATICALLY SEC-1")) {
                    assertEquals("alice", attr(a, "user"));
                    assertEquals(Long.toString(fired), a.get("created_at").toString());
                    assertEquals(Long.toString(resolved), a.get("updated_at").toString());
                }
                if (stateRule(a).startsWith("FIRED SEC-4")) {
                    assertEquals("web-1", attr(a, "host"));
                    assertEquals("8.000000000000000000", JsonAccess.requireMap(a, "value").get("metric_value"));
                }
                if (a.get("rule").toString().contains("SEC-")) {
                    assertTrue(Integer.parseInt(a.get("date").toString()) == 20261023, "date derived from event time in UTC");
                }
            }
            return null;
        });
    }
}
