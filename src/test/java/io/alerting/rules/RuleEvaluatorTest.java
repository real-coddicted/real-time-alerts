package io.alerting.rules;

import io.alerting.json.Json;
import io.alerting.model.AlertValue;
import io.alerting.model.Metric;
import io.alerting.model.NameValuePair;
import io.alerting.model.Rule;
import org.junit.jupiter.api.Test;
import io.alerting.threshold.ChainedThresholdEvaluator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuleEvaluatorTest {
    private final RuleEvaluator evaluator = new RuleEvaluator(ChainedThresholdEvaluator.standard());

    public static Rule rule(String json) {
        return Rule.fromJson(Json.parseObject(json));
    }

    // ratio < -0.20, resolve > -0.20 after 300ms, expiry 1000ms
    private final Rule withResolve = rule("""
            {"id":"R","subject":"PNL","scope":"UNIT",
             "fire":{"type":"RATIO","operator":"LT","value":"-0.20","expiryMs":1000},
             "resolve":{"type":"RATIO","operator":"GT","value":"-0.20","minTimeToResolveMs":300}}""");
    private final Rule noResolve = rule("""
            {"id":"N","subject":"PNL","scope":"UNIT","fire":{"type":"RATIO","operator":"LT","value":"-0.20","expiryMs":1000}}""");

    private static Metric m(String v) {
        return Metric.of("pnl", v, "base", "100");
    }

    private AlertValue firedValue(String v) {
        return evaluator.evaluate(m(v), withResolve, 0, null, false).value();
    }

    @Test
    void firesWithBreachEqualToSubjectAndRatioMetric() {
        EvaluationResult r = evaluator.evaluate(m("-30"), withResolve, 0, null, false);
        assertTrue(r.fired(), "fired");
        assertEquals(new NameValuePair("pnl", "-30"), r.value().breachValue());
        assertEquals(new NameValuePair("base", "100"), r.value().valueToCompare());
        assertEquals("-0.300000000000000000", r.value().metricValue());
    }

    @Test
    void notFiredWithoutActiveAlertIsQuiet() {
        EvaluationResult r = evaluator.evaluate(m("-10"), noResolve, 0, null, true);
        assertFalse(r.fired() || r.resolved() || r.expired(), "nothing");
    }

    @Test
    void dedupeIsNumericAndAgainstStoredValue() {
        AlertValue prev = firedValue("-30");
        EvaluationResult r = evaluator.evaluate(m("-30.000"), withResolve, 50, prev, true);
        assertEquals(EvaluationResult.NONE, r);
    }

    @Test
    void breachValueCarriesOver() {
        AlertValue prev = firedValue("-30");
        EvaluationResult r = evaluator.evaluate(m("-60"), withResolve, 10, prev, false);
        assertTrue(r.fired(), "still breaching");
        assertEquals("-30", r.value().breachValue().value());
        assertEquals("-60", r.value().subjectValue().value());
    }

    @Test
    void minTimeToResolveHoldsResolution() {
        AlertValue prev = firedValue("-30");
        EvaluationResult held = evaluator.evaluate(m("-10"), withResolve, 299, prev, true);
        assertTrue(held.fired() && !held.resolved(), "keepTicking -> fired, not resolved");
        EvaluationResult quiet = evaluator.evaluate(m("-10"), withResolve, 299, prev, false);
        assertFalse(quiet.fired() || quiet.resolved(), "no keepTicking -> nothing");
        EvaluationResult resolved = evaluator.evaluate(m("-10"), withResolve, 300, prev, false);
        assertTrue(resolved.resolved(), "resolves at min time");
    }

    @Test
    void resolveTakesPrecedenceOverExpiry() {
        AlertValue prev = firedValue("-30");
        assertTrue(evaluator.evaluate(m("-10"), withResolve, 5000, prev, false).resolved(), "resolved");
    }

    @Test
    void expiresWhenNotBreachingAfterExpiry() {
        AlertValue prev = firedValue("-30");
        assertFalse(evaluator.evaluate(m("-10"), noResolve, 999, prev, false).expired(), "before expiry");
        assertTrue(evaluator.evaluate(m("-10"), noResolve, 1000, prev, false).expired(), "at expiry");
        assertTrue(evaluator.evaluate(m("-40"), noResolve, 5000, prev, false).fired(), "breaching beats expiry");
    }

    @Test
    void negativeExpiryNeverExpires() {
        Rule r = rule("""
                {"id":"X","subject":"PNL","scope":"UNIT","fire":{"type":"RATIO","operator":"LT","value":"-0.20"}}""");
        AlertValue prev = evaluator.evaluate(m("-30"), r, 0, null, false).value();
        EvaluationResult res = evaluator.evaluate(m("-10"), r, Long.MAX_VALUE / 2, prev, true);
        assertTrue(res.fired() && !res.expired(), "keepTicking, no expiry");
    }
}
