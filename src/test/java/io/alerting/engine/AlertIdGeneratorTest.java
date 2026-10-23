package io.alerting.engine;

import io.alerting.model.EntityKey;
import io.alerting.model.Rule;
import io.alerting.testing.Test;

import java.util.Map;

import static io.alerting.rules.RuleEvaluatorTest.rule;
import static io.alerting.testing.Assert.assertEquals;
import static io.alerting.testing.Assert.assertNotEquals;
import static io.alerting.testing.Assert.assertThrows;
import static io.alerting.testing.Assert.assertTrue;

public class AlertIdGeneratorTest {
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
    void deterministicAndRuleSensitive() {
        AlertIdGenerator g = new AlertIdGenerator(6);
        assertEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("s", R1, KEY, 1L, 0L));
        assertNotEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("s", R2, KEY, 1L, 0L));
        assertNotEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("other", R1, KEY, 1L, 0L));
        assertNotEquals(g.generate("s", R1, KEY, 1L, 0L), g.generate("s", R1, KEY, 1L, 5L));
    }
}
