package io.alerting.config;

import io.alerting.json.Json;
import io.alerting.testing.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static io.alerting.testing.Assert.assertEquals;
import static io.alerting.testing.Assert.assertThrows;

public class JsonTest {
    @Test
    void roundTripPreservesStructureAndDecimalScale() {
        String text = "{\"a\":\"x\\n\\\"y\\\"\",\"b\":5983719.7500,\"c\":[1,true,null,{\"d\":-1.5E+3}],\"e\":{}}";
        Object parsed = Json.parse(text);
        Map<?, ?> m = (Map<?, ?>) parsed;
        assertEquals("x\n\"y\"", m.get("a"));
        assertEquals("5983719.7500", ((BigDecimal) m.get("b")).toPlainString());
        assertEquals(4, ((List<?>) m.get("c")).size());
        assertEquals("{\"a\":\"x\\n\\\"y\\\"\",\"b\":5983719.7500,\"c\":[1,true,null,{\"d\":-1500}],\"e\":{}}", Json.write(parsed));
    }

    @Test
    void unicodeEscapes() {
        assertEquals("é", Json.parse("\"\\u00e9\""));
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1} x"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("[1,]"));
    }
}
