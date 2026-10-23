package io.alerting.pipeline;

import io.alerting.config.PipelineConfig;
import io.alerting.json.Json;
import io.alerting.rules.RuleCatalog;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Timer behavior of the stateful DoFn: throttling (S1) and end-of-day expiry (S5). */
class AlertingDoFnTest {
    private static final long INTERVAL = 100;

    private static final String CONFIG = "{"
            + "\"engine\":{\"outputIntervalMs\":" + INTERVAL + ",\"keepTicking\":true,\"zone\":\"America/New_York\"},"
            + "\"rules\":{\"type\":\"inline\",\"rules\":[{\"id\":\"R\",\"subject\":\"PNL\",\"scope\":\"UNIT\","
            + "  \"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"-0.20\",\"expiryMs\":-1}}]},"
            + "\"kafka\":{\"bootstrapServers\":\"unused:9092\"},"
            + "\"sources\":[{\"name\":\"src\",\"topic\":\"t\"}],"
            + "\"sink\":{\"topic\":\"alerts\"},"
            + "\"streams\":[{\"name\":\"ticks\",\"source\":\"src\",\"timestamp\":\"ts\","
            + "  \"key\":{\"scope\":\"UNIT\",\"date\":\"date\",\"attributes\":{\"unit\":\"unit\"}},"
            + "  \"subjects\":{\"PNL\":{\"subject\":{\"name\":\"pnl\",\"value\":\"pnl\"},\"compare\":{\"name\":\"base\",\"value\":{\"const\":\"100\"}}}}}]"
            + "}";

    private static KV<String, Event> tick(String pnl, long ts) {
        return KV.of("src", new Event("{\"unit\":\"u1\",\"date\":20240918,\"pnl\":\"" + pnl + "\",\"ts\":" + ts + "}", 0L));
    }

    private static List<Map<String, Object>> alertsOf(Iterable<KV<String, String>> records) {
        List<Map<String, Object>> out = new ArrayList<>();
        records.forEach(kv -> out.add(Json.parseObject(kv.getValue())));
        return out;
    }

    private static PCollection<KV<String, String>> run(Pipeline p, PipelineConfig config, TestStream<KV<String, Event>> events) throws Exception {
        PCollection<KV<String, Event>> input = p.apply(events);
        PCollectionView<RuleCatalog> rules = PipelineTestSupport.staticRules(p, config);
        return AlertingPipeline.alerts(config, PipelineTestSupport.sources(config, input), rules);
    }

    @Test
    void s1BurstIsEvaluatedOnceWithLatestValueAndFirstEventTime() throws Exception {
        PipelineConfig config = PipelineConfig.fromJson(CONFIG);
        Pipeline p = PipelineTestSupport.pipeline();
        TestStream<KV<String, Event>> events = PipelineTestSupport.stream()
                .advanceProcessingTime(Duration.millis(1))
                .addElements(tick("-30", 1_000), tick("-50", 2_000))
                .advanceProcessingTime(Duration.millis(INTERVAL + 1))
                .advanceWatermarkToInfinity();
        PAssert.that(run(p, config, events)).satisfies(records -> {
            List<Map<String, Object>> alerts = alertsOf(records);
            assertEquals(1, alerts.size(), "one evaluation for the burst");
            Map<String, Object> a = alerts.get(0);
            assertEquals("FIRED", a.get("state"));
            assertEquals("-50", ((Map<?, ?>) ((Map<?, ?>) a.get("value")).get("subject_value")).get("value"), "latest event evaluated");
            assertEquals("1000", a.get("created_at").toString(), "stamped with the first event's time");
            return null;
        });
        p.run().waitUntilFinish();
    }

    @Test
    void s5EndOfDayExpiresAndKeyStartsFresh() throws Exception {
        PipelineConfig config = PipelineConfig.fromJson(CONFIG);
        Pipeline p = PipelineTestSupport.pipeline();
        TestStream<KV<String, Event>> events = PipelineTestSupport.stream()
                .advanceProcessingTime(Duration.millis(1))
                .addElements(tick("-30", 1_000))
                .advanceProcessingTime(Duration.millis(INTERVAL + 1))
                .advanceProcessingTime(Duration.standardHours(25))
                .addElements(tick("-31", 5_000))
                .advanceProcessingTime(Duration.millis(INTERVAL + 1))
                .advanceWatermarkToInfinity();
        PAssert.that(run(p, config, events)).satisfies(records -> {
            List<Map<String, Object>> alerts = alertsOf(records);
            List<String> states = new ArrayList<>();
            alerts.forEach(a -> states.add(a.get("state").toString()));
            states.sort(null);
            assertEquals(List.of("FIRED", "FIRED", "RESOLVED_EXPIRED"), states);
            String firstId = null;
            for (Map<String, Object> a : alerts) {
                if (a.get("created_at").toString().equals("1000")) {
                    if (firstId == null) firstId = a.get("alert_id").toString();
                    assertEquals(firstId, a.get("alert_id"), "expiry belongs to the first alert");
                }
            }
            return null;
        });
        p.run().waitUntilFinish();
    }
}
