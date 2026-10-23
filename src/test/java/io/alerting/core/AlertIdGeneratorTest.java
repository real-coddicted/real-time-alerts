package io.alerting.core;

import io.alerting.model.EntityKey;
import io.alerting.model.Rule;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.alerting.rules.RuleEvaluatorTest.rule;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Spec T22..T25. */
class AlertIdGeneratorTest {
    private static final Rule R1 = rule("{\"id\":\"R1\",\"subject\":\"S\",\"scope\":\"UNIT\",\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"1\"}}");
    private static final Rule R2 = rule("{\"id\":\"R2\",\"subject\":\"S\",\"scope\":\"UNIT\",\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"1\"}}");
    private static final EntityKey KEY = EntityKey.of("UNIT", 20240918, Map.of("unit_id", 1089));

    @Test
    void formatMatchesSpec() {
        String id8 = new AlertIdGenerator(8).generate("s", R1, KEY, 1748975629457L, 0L);
        assertTrue(id8.matches("[A-Za-z0-9_-]{11}:1748975629457"), id8);
        String id6 = new AlertIdGenerator(6).generate("s", R1, KEY, 1748727898000L, 0L);
        assertTrue(id6.matches("[A-Za-z0-9_-]{8}:1748727898000"), id6);
    }

    @Test
    void boundsAreChecked() {
        assertThrows(IllegalArgumentException.class, () -> new AlertIdGenerator(0));
        assertThrows(IllegalArgumentException.class, () -> new AlertIdGenerator(33));
    }

    @Test
    void deterministicAndInputSensitive() {
        AlertIdGenerator g = new AlertIdGenerator(6);
        assertEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("s", R1, KEY, 1L, 0L));
        assertNotEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("s", R2, KEY, 1L, 0L));
        assertNotEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("other", R1, KEY, 1L, 0L));
        assertNotEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("s", R1, KEY, 1L, 5L), "re-fire at same event time");
    }

    @Test
    void endOfDayInZone() {
        long now = java.time.Instant.parse("2024-09-18T14:00:00Z").toEpochMilli();
        java.time.ZoneId ny = java.time.ZoneId.of("America/New_York");
        assertEquals(java.time.Instant.parse("2024-09-19T03:59:59.999Z").toEpochMilli() - now, EndOfDay.millisUntilEndOfDay(20240918, now, ny));
        assertEquals(java.time.Instant.parse("2024-09-21T03:59:59.999Z").toEpochMilli() - now, EndOfDay.millisUntilEndOfDay(20240920, now, ny), "future date");
        assertEquals(java.time.Instant.parse("2024-09-19T03:59:59.999Z").toEpochMilli() - now, EndOfDay.millisUntilEndOfDay(20240823, now, ny), "past date -> today");
    }
}
