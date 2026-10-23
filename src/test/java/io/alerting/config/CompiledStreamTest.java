package io.alerting.config;

import io.alerting.json.Json;
import io.alerting.model.EntityKey;
import io.alerting.model.Metric;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Spec T10..T16, expressed through the declarative streams of examples/risk/pipeline.json. */
class CompiledStreamTest {
    private static final Map<String, CompiledStream> STREAMS = load();

    private static Map<String, CompiledStream> load() {
        try {
            PipelineConfig config = PipelineConfig.load("examples/risk/pipeline.json", Map.of());
            Map<String, CompiledStream> m = new HashMap<>();
            config.streams().forEach(s -> {
                CompiledStream c = CompiledStream.compile(s);
                m.put(c.name(), c);
            });
            return m;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Map<String, Object> obj(String json) {
        return Json.parseObject(json);
    }

    private static Map<String, Object> equity(int unit, String margin, String equity, int org) {
        return obj("{\"date\":20240918,\"unit_id\":" + unit + ",\"unit_type\":\"UNIT_TYPE_ENTITY\",\"margin_type\":\"" + margin
                + "\",\"equity\":\"" + equity + "\",\"org_ids\":[" + org + "],\"issuers\":{\"10\":{\"market_capitalization\":{\"value\":\"2900000000\"}}}}");
    }

    @Test
    void t10LiquidationEligibilityFilter() {
        CompiledStream s = STREAMS.get("liquidation-non-large-cap");
        assertEquals(1, s.prepare(equity(1000, "MARGIN_TYPE_REGT", "4000000", 25)).size(), "eligible");
        assertEquals(0, s.prepare(equity(1000, "MARGIN_TYPE_REGT", "4000000", 26)).size(), "wrong org");
        assertEquals(0, s.prepare(equity(1000, "MARGIN_TYPE_REGT", "5000000", 25)).size(), "equity not < 5M");
        assertEquals(0, s.prepare(equity(1089, "MARGIN_TYPE_REGT", "4000000", 25)).size(), "disallowed entity");
        assertEquals(0, s.prepare(equity(1000, "MARGIN_TYPE_PM", "4000000", 25)).size(), "not REGT");
    }

    @Test
    void t11t12CapSplitIsExhaustive() {
        Predicate<Map<String, Object>> large = Predicates.compile(obj("{\"field\":\"cap\",\"op\":\"gte\",\"value\":\"3000000000\"}"));
        for (String cap : List.of("2900000000", "3000000000", "3100000000")) {
            assertEquals(!cap.equals("2900000000"), large.test(obj("{\"cap\":\"" + cap + "\"}")), cap);
        }
        assertFalse(large.test(obj("{}")), "missing cap counts as 0 -> non large");
    }

    @Test
    void t13ExplodeIssuersCopiesNettingAndIssuer() {
        Map<String, Object> eq = obj("{\"date\":20250101,\"unit_id\":42,\"unit_type\":\"UNIT_TYPE_ENTITY\",\"margin_type\":\"MARGIN_TYPE_REGT\","
                + "\"netting_id\":\"N1\",\"org_ids\":[25],\"equity\":\"-100\",\"issuers\":{"
                + "\"10\":{\"market_capitalization\":{\"value\":\"1000\"},\"market_value\":{\"gross_market_value\":\"5\"}},"
                + "\"20\":{\"market_capitalization\":{\"value\":\"2000\"},\"market_value\":{\"gross_market_value\":\"8\"}}}}");
        CompiledStream s = STREAMS.get("liquidation-non-large-cap");
        List<Map<String, Object>> items = s.prepare(eq);
        assertEquals(2, items.size());
        assertEquals(EntityKey.of("UNIT", 20250101, Map.of("unit_id", 42, "unit_type", "UNIT_TYPE_ENTITY",
                "margin_type", "MARGIN_TYPE_REGT", "underlying_instrument_id", "10", "netting_id", "N1")), s.key(items.get(0)));
        assertEquals("20", s.key(items.get(1)).attribute("underlying_instrument_id"));
        assertEquals(Metric.of("equity", "-100", "gross_market_value", "8"), s.metric("NON_LARGE_CAP_LIQUIDATION", items.get(1)));
    }

    @Test
    void t14PmBarrierFilteredAndPayloadUnwrapped() {
        CompiledStream s = STREAMS.get("pm-excess");
        assertEquals(0, s.prepare(obj("{\"payload\":{\"message_type\":\"PM_RESULT_MESSAGE_TYPE_START_BARRIER\"}}")).size());
        List<Map<String, Object>> items = s.prepare(obj("{\"payload\":{\"message_type\":\"PM_RESULT_MESSAGE_TYPE_MESSAGE\","
                + "\"value\":{\"date\":20240918,\"entity_id\":2173,\"excess\":\"-5214.04\",\"equity\":\"-292.24\"}}}"));
        assertEquals(1, items.size());
        assertEquals(EntityKey.of("UNIT", 20240918, Map.of("unit_id", 2173, "unit_type", "UNIT_TYPE_ENTITY", "margin_type", "MARGIN_TYPE_PM")),
                s.key(items.get(0)));
        assertEquals(Metric.of("excess", "-5214.04", "equity", "-292.24"), s.metric("EXCESS", items.get(0)));
    }

    @Test
    void t15EquityKey() {
        CompiledStream s = STREAMS.get("equity-unit");
        Map<String, Object> item = s.prepare(obj("{\"date\":20240918,\"unit_id\":1089,\"unit_type\":\"ENTITY\",\"margin_type\":\"REGT\",\"netting_id\":\"CSTCSEL\"}")).get(0);
        assertEquals(EntityKey.of("UNIT", 20240918, Map.of("unit_id", 1089, "unit_type", "ENTITY", "margin_type", "REGT", "netting_id", "CSTCSEL")), s.key(item));
    }

    @Test
    void t16PnlKeyAndRawVsNormalizedSubjects() {
        CompiledStream s = STREAMS.get("pnl-position");
        Map<String, Object> item = s.prepare(obj("{\"date\":20240918,\"unit_id\":105570,\"unit_type\":\"ACCOUNT\",\"margin_type\":\"CASH\","
                + "\"instrument_id\":{\"value\":775616},\"netting_id\":\"CST\",\"total_pnl\":\"-64766.25\",\"sod_equity\":\"5918953.5\","
                + "\"price\":{\"value\":\"43.885\"},\"sod_price\":{\"value\":\"43.41\"},\"gross_market_value\":\"5983719.7500\",\"sod_gmv\":\"5918953.5000\"}")).get(0);
        assertEquals(EntityKey.of("POSITION", 20240918, Map.of("unit_id", 105570, "unit_type", "ACCOUNT", "margin_type", "CASH",
                "instrument_id", 775616, "netting_id", "CST")), s.key(item));
        assertEquals(Metric.of("total_pnl", "-64766.25", "sod_equity", "5918953.5"), s.metric("TOTAL_PNL", item));
        assertEquals(Metric.of("price", "43.885", "sod_price", "43.41"), s.metric("PRICE", item));
        assertEquals(Metric.of("gross_market_value", "5983719.7500", "sod_gross_market_value", "5918953.5000"), s.metric("MARKET_VALUE", item));
    }

    @Test
    void regtExcessIsMinOfHouseExcessAndSma() {
        CompiledStream s = STREAMS.get("regt-excess");
        Map<String, Object> item = s.prepare(obj("{\"date\":20240918,\"unit_id\":1,\"house_excess\":\"-5686.4826\",\"sma\":\"-2757.096305\",\"equity\":\"-5686.4826\"}")).get(0);
        assertEquals(Metric.of("excess", "-5686.4826", "equity", "-5686.4826"), s.metric("EXCESS", item));
        assertEquals("MARGIN_TYPE_REGT", s.key(item).attribute("margin_type"));
    }

    @Test
    void pmV2DefaultsToZeroWithoutSummary() {
        CompiledStream s = STREAMS.get("pmv2-excess");
        assertEquals(Metric.of("excess", "0", "equity", "0"), s.metric("EXCESS", s.prepare(obj("{\"date\":20240918,\"unit_id\":1}")).get(0)));
    }

    @Test
    void dateAndTimestampParsing() {
        assertEquals(0, CompiledStream.toDate(null));
        assertEquals(20240918, CompiledStream.toDate("2024-09-18"));
        assertEquals(1724439983468L, CompiledStream.toEpochMillis("1724439983468"));
        assertEquals(1724439983468L, CompiledStream.toEpochMillis("2024-08-23T19:06:23.468Z"));
    }

    @Test
    void configValidationAndInterpolation() {
        String base = "{\"rules\":{\"type\":\"inline\",\"rules\":[]},\"kafka\":{\"bootstrapServers\":\"${BOOT:-k:9092}\"},"
                + "\"sources\":[{\"name\":\"a\",\"topic\":\"t\"}],\"sink\":{\"topic\":\"o\"},\"streams\":[%s]}";
        String stream = "{\"name\":\"s\",\"source\":\"%s\",\"key\":{\"scope\":\"U\"},\"subjects\":{\"X\":{\"subject\":{\"name\":\"x\",\"value\":\"x\"}}}}";
        PipelineConfig ok = PipelineConfig.fromJson(String.format(base, String.format(stream, "a")));
        assertEquals("k:9092", ok.kafka().get("bootstrapServers"));
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.fromJson(String.format(base, String.format(stream, "nope"))), "unknown source");
        assertThrows(IllegalArgumentException.class,
                () -> PipelineConfig.fromJson(String.format(base, String.format(stream, "a") + "," + String.format(stream, "a"))), "duplicate stream");
        assertThrows(IllegalArgumentException.class, () -> PipelineConfig.fromJson("{\"x\":\"${SURELY_UNSET_VAR_12345}\"}"), "unset variable");
    }

    @Test
    void securityExampleCompiles() throws Exception {
        PipelineConfig config = PipelineConfig.load("examples/security/pipeline.json", Map.of());
        CompiledStream egress = CompiledStream.compile(config.streams().get(1));
        List<Map<String, Object>> items = egress.prepare(obj("{\"direction\":\"outbound\",\"site\":\"dc1\",\"hosts\":["
                + "{\"name\":\"web-1\",\"env\":\"prod\",\"bytes_out\":9000000000,\"baseline_bytes_out\":1000000000}]}"));
        assertEquals(EntityKey.of("HOST", 0, Map.of("host", "web-1", "env", "prod", "site", "dc1")), egress.key(items.get(0)));
        assertTrue(config.rules().get("path").toString().endsWith("examples/security/rules.json"), "rules path resolved");
    }
}
