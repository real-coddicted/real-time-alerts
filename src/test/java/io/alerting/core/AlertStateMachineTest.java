package io.alerting.core;

import io.alerting.model.Alert;
import io.alerting.model.AlertState;
import io.alerting.model.EntityKey;
import io.alerting.model.Metric;
import io.alerting.model.Rule;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.alerting.rules.RuleEvaluatorTest.rule;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Transitions of spec 5.5 plus scenarios S2, S3, S4 and the rule-lifecycle fixes, without a runner. */
class AlertStateMachineTest {
    private static final EntityKey KEY = EntityKey.of("UNIT", 20240918, Map.of("unit_id", "u1"));
    private static final String RESOLVE = "\"resolve\":{\"type\":\"RATIO\",\"operator\":\"GT\",\"value\":\"-0.20\",\"minTimeToResolveMs\":5000}";

    private final Map<String, Alert> active = new LinkedHashMap<>();

    static Rule fireBelow(String id, String threshold, long expiryMs, String extra) {
        return rule("{\"id\":\"" + id + "\",\"subject\":\"PNL\",\"scope\":\"UNIT\""
                + ",\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"" + threshold + "\",\"expiryMs\":" + expiryMs + "}"
                + (extra.isEmpty() ? "" : "," + extra) + "}");
    }

    private static AlertStateMachine machine(boolean keepTicking) {
        return new AlertStateMachine(EngineConfig.builder().keepTicking(keepTicking).build());
    }

    private List<Alert> step(AlertStateMachine m, List<Rule> rules, String pnl, long now) {
        return m.evaluate("s", KEY, rules, active, subject -> Metric.of("pnl", pnl, "base", "100"), now, now).emitted();
    }

    private static List<AlertState> states(List<Alert> alerts) {
        return alerts.stream().map(Alert::state).collect(Collectors.toList());
    }

    @Test
    void firesOnceThenUpdatesWithStableId() {
        AlertStateMachine m = machine(true);
        List<Rule> rules = List.of(fireBelow("R", "-0.20", 14_400_000, ""));
        Alert fired = step(m, rules, "-30", 1000).get(0);
        assertEquals(AlertState.FIRED, fired.state());
        List<Alert> updated = step(m, rules, "-40", 2000);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(updated));
        assertEquals(fired.alertId(), updated.get(0).alertId());
        assertEquals(1000L, updated.get(0).createdAt());
        assertEquals(2000L, updated.get(0).updatedAt());
        assertEquals("-30", updated.get(0).value().breachValue().value(), "breach carried from the fire");
    }

    @Test
    void s2RuleExpiryResolvesWithFireTimeValue() {
        AlertStateMachine m = machine(true);
        List<Rule> rules = List.of(fireBelow("R", "-0.20", 1000, ""));
        step(m, rules, "-30", 0);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(step(m, rules, "-10", 999)), "before expiry, keepTicking");
        List<Alert> out = step(m, rules, "-11", 1000);
        assertEquals(List.of(AlertState.RESOLVED_EXPIRED), states(out));
        assertEquals("-30", out.get(0).value().subjectValue().value());
        assertTrue(active.isEmpty(), "state cleared");
    }

    @Test
    void s3MinTimeToResolve() {
        List<Rule> rules = List.of(fireBelow("R", "-0.20", 14_400_000, RESOLVE));
        AlertStateMachine ticking = machine(true);
        step(ticking, rules, "-30", 0);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(step(ticking, rules, "-10", 1000)));
        assertEquals(List.of(AlertState.RESOLVED_AUTOMATICALLY), states(step(ticking, rules, "-11", 5000)));

        active.clear();
        AlertStateMachine quiet = machine(false);
        step(quiet, rules, "-30", 0);
        assertEquals(List.of(), states(step(quiet, rules, "-10", 1000)));
    }

    @Test
    void s4DedupeAgainstFireValue() {
        AlertStateMachine m = machine(true);
        List<Rule> rules = List.of(fireBelow("R", "-0.20", 14_400_000, ""));
        step(m, rules, "-30", 0);
        assertEquals(List.of(), states(step(m, rules, "-30.00", 10)));
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(step(m, rules, "-40", 20)));
        // Spec semantics (K4): the stored value stays the fire value, so a stable -40 keeps re-emitting.
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(step(m, rules, "-40", 30)));
    }

    @Test
    void dedupeAgainstLastEmittedOption() {
        AlertStateMachine m = new AlertStateMachine(EngineConfig.builder().keepTicking(true).dedupeAgainstLastEmitted(true).build());
        List<Rule> rules = List.of(fireBelow("R", "-0.20", 14_400_000, ""));
        step(m, rules, "-30", 0);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(step(m, rules, "-40", 20)));
        assertEquals(List.of(), states(step(m, rules, "-40", 30)));
    }

    @Test
    void ruleRemovalResolves() {
        AlertStateMachine m = machine(true);
        step(m, List.of(fireBelow("R", "-0.20", 14_400_000, "")), "-30", 0);
        assertEquals(List.of(AlertState.RESOLVED_RULE_REMOVED), states(step(m, List.of(), "-40", 10)));
        assertTrue(active.isEmpty(), "state cleared");

        AlertStateMachine legacy = new AlertStateMachine(EngineConfig.builder().keepTicking(true).resolveOnRuleRemoval(false).build());
        step(legacy, List.of(fireBelow("R", "-0.20", 14_400_000, "")), "-30", 0);
        assertEquals(List.of(), states(step(legacy, List.of(), "-40", 10)));
        assertEquals(1, active.size(), "orphaned until end of day, as in the spec");
    }

    @Test
    void editingARuleKeepsTheAlert() {
        AlertStateMachine m = machine(true);
        Alert fired = step(m, List.of(fireBelow("R", "-0.20", 14_400_000, "")), "-30", 0).get(0);
        Rule v2 = rule("{\"id\":\"R\",\"version\":2,\"subject\":\"PNL\",\"scope\":\"UNIT\",\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"-0.25\"}}");
        List<Alert> out = step(m, List.of(v2), "-40", 10);
        assertEquals(List.of(AlertState.FIRED_UPDATED), states(out));
        assertEquals(fired.alertId(), out.get(0).alertId());
        assertEquals(2L, out.get(0).rule().version());
    }

    @Test
    void poisonValueFailsOnlyThatRule() {
        AlertStateMachine m = machine(true);
        AlertStateMachine.Outcome o = m.evaluate("s", KEY, List.of(fireBelow("R", "-0.20", -1, "")), active,
                subject -> Metric.of("pnl", "not-a-number", "base", "100"), 0, 0);
        assertEquals(1, o.errors());
        assertEquals(List.of(), o.emitted());
    }

    @Test
    void endOfDayExpiresEverything() {
        AlertStateMachine m = machine(true);
        List<Rule> rules = List.of(fireBelow("A", "-0.20", -1, ""), fireBelow("B", "-0.25", -1, ""));
        assertEquals(2, step(m, rules, "-30", 0).size());
        List<Alert> out = m.expireAll(active, 99);
        assertEquals(List.of(AlertState.RESOLVED_EXPIRED, AlertState.RESOLVED_EXPIRED), states(out));
        assertEquals(99L, out.get(0).updatedAt());
        assertTrue(active.isEmpty(), "cleared");
    }
}
