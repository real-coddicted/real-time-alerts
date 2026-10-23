package io.alerting.engine;

import io.alerting.config.ConfigLoader;
import io.alerting.config.EngineSetup;
import io.alerting.json.Json;
import io.alerting.json.JsonAccess;
import io.alerting.model.Alert;
import io.alerting.model.AlertState;
import io.alerting.sink.InMemorySink;
import io.alerting.testing.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.alerting.testing.Assert.assertEquals;
import static io.alerting.testing.Assert.assertNotEquals;

/**
 * The spec's end-to-end scenario (12.4, E1..E17) replayed through examples/risk: the original 7-branch
 * pipeline expressed purely as config. Asserts states and rule ids per step, not just cumulative counts.
 */
public class RiskScenarioTest {
    private static final Path DIR = Path.of("examples/risk");

    @Test
    @SuppressWarnings("unchecked")
    void replayE1toE17() throws Exception {
        EngineSetup setup = ConfigLoader.load(DIR.resolve("engine.json"));
        ManualScheduler scheduler = new ManualScheduler(Instant.parse("2024-09-18T14:00:00Z").toEpochMilli());
        InMemorySink sink = new InMemorySink();
        AlertEngine engine = AlertEngine.builder()
                .config(setup.config())
                .ruleProvider(setup.ruleProvider())
                .streams(setup.streams())
                .sink(sink)
                .scheduler(scheduler)
                .build();
        engine.start();
        scheduler.runPending();
        assertEquals(21, engine.rules().size(), "rules R5..R25 loaded");

        long step = setup.config().outputInterval().toMillis() + 1;
        Map<String, List<Alert>> byStep = new HashMap<>();
        for (String line : Files.readAllLines(DIR.resolve("scenario.jsonl"))) {
            if (line.isBlank() || line.startsWith("//")) continue;
            Map<String, Object> env = Json.parseObject(line);
            String name = JsonAccess.requireString(env, "step");
            int before = sink.size();
            engine.submit(JsonAccess.requireString(env, "source"), env.get("record"));
            scheduler.advance(step);
            List<Alert> produced = sink.alerts().subList(before, sink.size());
            byStep.put(name, produced);

            Map<String, Object> expect = JsonAccess.requireMap(env, "expect");
            List<String> expected = new ArrayList<>();
            for (Object pair : (List<Object>) expect.get("new")) {
                List<Object> p = (List<Object>) pair;
                expected.add(p.get(0) + " " + p.get(1));
            }
            List<String> actual = produced.stream().map(a -> a.state() + " " + a.rule().id()).collect(Collectors.toCollection(ArrayList::new));
            expected.sort(null);
            actual.sort(null);
            assertEquals(expected, actual, name + " alerts");
            assertEquals(JsonAccess.getInt(expect, "total", -1), sink.size(), name + " total");
        }

        // E4: ratio is exactly -1, fires R13 (< -0.90) but not R14 (< -1, strict).
        assertEquals("-1.000000000000000000", byStep.get("E4").get(0).value().metricValue());

        // E6 -> E7 -> E8 are one alert's lifecycle.
        Alert e6 = byStep.get("E6").get(0);
        assertEquals(e6.alertId(), byStep.get("E7").get(0).alertId());
        Alert e8 = byStep.get("E8").get(0);
        assertEquals(e6.alertId(), e8.alertId());
        assertEquals("-300.91", e8.value().breachValue().value(), "breach carried from fire");
        assertEquals("-30.91", e8.value().subjectValue().value());
        assertEquals(1724459983468L, e8.updatedAt(), "event time basis");
        // E12 re-fires R18 on the same key with the same event time: still a new alert id.
        Alert e12 = byStep.get("E12").stream().filter(a -> a.rule().id().equals("R18")).findFirst().orElseThrow();
        assertEquals(e6.createdAt(), e12.createdAt());
        assertNotEquals(e6.alertId(), e12.alertId());

        // E17: large-cap liquidation fires per issuer, with netting carried onto the key.
        Set<String> issuers = byStep.get("E17").stream()
                .filter(a -> a.rule().id().equals("R25"))
                .map(a -> a.key().attribute("underlying_instrument_id"))
                .collect(Collectors.toSet());
        assertEquals(Set.of("227", "873"), issuers);
        assertEquals(Set.of("CST"), byStep.get("E17").stream().map(a -> a.key().attribute("netting_id")).collect(Collectors.toSet()));

        assertEquals(0L, engine.metrics().evaluationErrors.sum());
        assertEquals(21L, engine.metrics().alerts(AlertState.FIRED));
        engine.close();
    }
}
