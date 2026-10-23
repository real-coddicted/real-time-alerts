package io.alerting.rules;

import io.alerting.model.EntityKey;
import io.alerting.model.Rule;
import io.alerting.testing.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.alerting.rules.RuleEvaluatorTest.rule;
import static io.alerting.testing.Assert.assertEquals;
import static io.alerting.testing.Assert.assertFalse;
import static io.alerting.testing.Assert.assertTrue;

public class RuleCatalogTest {
    private static final String FIRE = "\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"-0.2\"}";

    private static final EntityKey POSITION = EntityKey.of("POSITION", 20240918, Map.of(
            "unit_id", 105570, "unit_type", "ACCOUNT", "margin_type", "CASH", "instrument_id", 775616, "netting_id", "CST"));

    @Test
    void absentAttributesAreWildcards() {
        assertTrue(RuleMatcher.matches(rule("{\"id\":\"a\",\"subject\":\"S\",\"scope\":\"POSITION\"," + FIRE + "}"), POSITION), "no constraints");
        assertTrue(RuleMatcher.matches(rule("{\"id\":\"a\",\"subject\":\"S\",\"scope\":\"POSITION\",\"match\":{\"unit_id\":\"105570\"}," + FIRE + "}"), POSITION),
                "string rule value matches numeric key value");
        assertTrue(RuleMatcher.matches(rule("{\"id\":\"a\",\"subject\":\"S\",\"scope\":\"POSITION\",\"match\":{\"margin_type\":[\"PM\",\"CASH\"]}," + FIRE + "}"), POSITION),
                "list of allowed values");
    }

    @Test
    void mismatches() {
        assertFalse(RuleMatcher.matches(rule("{\"id\":\"a\",\"subject\":\"S\",\"scope\":\"UNIT\"," + FIRE + "}"), POSITION), "scope");
        assertFalse(RuleMatcher.matches(rule("{\"id\":\"a\",\"subject\":\"S\",\"scope\":\"POSITION\",\"match\":{\"unit_id\":1}," + FIRE + "}"), POSITION), "unit");
        assertFalse(RuleMatcher.matches(rule("{\"id\":\"a\",\"subject\":\"S\",\"scope\":\"POSITION\",\"match\":{\"desk\":\"X\"}," + FIRE + "}"), POSITION),
                "attribute missing from key");
    }

    @Test
    void canMatchOnAnyKeyAttributeIncludingNetting() {
        assertTrue(RuleMatcher.matches(rule("{\"id\":\"a\",\"subject\":\"S\",\"scope\":\"POSITION\",\"match\":{\"netting_id\":\"CST\"}," + FIRE + "}"), POSITION), "netting");
    }

    @Test
    void catalogFiltersBySubjectAndDropsInactive() {
        List<Rule> rules = List.of(
                rule("{\"id\":\"r1\",\"subject\":\"TOTAL_PNL\",\"scope\":\"POSITION\"," + FIRE + "}"),
                rule("{\"id\":\"r2\",\"subject\":\"PRICE\",\"scope\":\"POSITION\"," + FIRE + "}"),
                rule("{\"id\":\"r3\",\"subject\":\"TOTAL_PNL\",\"scope\":\"POSITION\",\"active\":false," + FIRE + "}"),
                rule("{\"id\":\"r4\",\"subject\":\"TOTAL_PNL\",\"scope\":\"UNIT\"," + FIRE + "}"));
        RuleCatalog c = RuleCatalog.of(1, rules);
        assertEquals(List.of("r1"), c.match(POSITION, Set.of("TOTAL_PNL")).stream().map(Rule::id).toList());
        assertEquals(List.of("r1", "r2"), c.match(POSITION, Set.of("TOTAL_PNL", "PRICE")).stream().map(Rule::id).toList());
    }

    @Test
    void duplicateIdsKeepHighestVersion() {
        RuleCatalog c = RuleCatalog.of(1, List.of(
                rule("{\"id\":\"r\",\"version\":2,\"subject\":\"S\",\"scope\":\"U\"," + FIRE + "}"),
                rule("{\"id\":\"r\",\"version\":1,\"subject\":\"S\",\"scope\":\"U\"," + FIRE + "}")));
        assertEquals(2L, c.get("r").version());
    }

    @Test
    void parserSkipsInvalidRules() {
        List<Rule> rules = RulesParser.parse("""
                {"rules":[
                  {"id":"ok","subject":"S","scope":"U","fire":{"type":"RATIO","operator":"LT","value":"1"}},
                  {"id":"bad-op","subject":"S","scope":"U","fire":{"type":"RATIO","operator":"OPERATOR_INVALID","value":"1"}},
                  {"id":"bad-value","subject":"S","scope":"U","fire":{"type":"RATIO","operator":"LT","value":"abc"}},
                  {"id":"no-fire","subject":"S","scope":"U"}
                ]}""");
        assertEquals(List.of("ok"), rules.stream().map(Rule::id).toList());
    }

    @Test
    void ruleJsonRoundTrip() {
        Rule r = rule("""
                {"id":"R18","version":3,"subject":"TOTAL_PNL","scope":"POSITION","match":{"margin_type":["CASH","PM"]},"severity":"HIGH",
                 "fire":{"type":"RATIO","operator":"LT","value":"-0.20","expiryMs":14400000},
                 "resolve":{"type":"RATIO","operator":"GT","value":"-0.20","minTimeToResolveMs":300}}""");
        assertEquals(r, Rule.fromJson(r.toJson()));
    }
}
