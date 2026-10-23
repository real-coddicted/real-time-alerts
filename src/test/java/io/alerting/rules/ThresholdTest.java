package io.alerting.rules;

import io.alerting.model.Operator;
import io.alerting.model.Threshold;
import io.alerting.testing.Test;
import io.alerting.threshold.ChainedThresholdEvaluator;
import io.alerting.threshold.ThresholdResult;

import java.math.BigDecimal;

import static io.alerting.testing.Assert.assertEquals;
import static io.alerting.testing.Assert.assertFalse;
import static io.alerting.testing.Assert.assertNull;
import static io.alerting.testing.Assert.assertThrows;
import static io.alerting.testing.Assert.assertTrue;

public class ThresholdTest {
    private final ChainedThresholdEvaluator chain = ChainedThresholdEvaluator.standard();

    private static Threshold t(String type, String op, String v) {
        return new Threshold(type, Operator.parse(op), new BigDecimal(v));
    }

    private ThresholdResult eval(Threshold t, String value, String cmp) {
        return chain.evaluate(t, new BigDecimal(value), new BigDecimal(cmp));
    }

    @Test
    void ratioUsesAbsoluteDenominatorAndScale18() {
        ThresholdResult r = eval(t("RATIO", "LT", "-0.10"), "-11", "-100");
        assertTrue(r.met(), "ratio -0.11 < -0.10");
        assertEquals("-0.110000000000000000", r.metric().toPlainString());
    }

    @Test
    void relative() {
        ThresholdResult r = eval(t("THRESHOLD_TYPE_RELATIVE", "OPERATOR_LESS_THAN", "-0.20"), "33.955", "43.41");
        assertTrue(r.met(), "(33.955 - 43.41) / 43.41 = -0.2178 < -0.20");
        assertEquals("-0.217806956922368118", r.metric().toPlainString());
    }

    @Test
    void zeroCompareNeverMeetsAndHasNoMetric() {
        assertFalse(eval(t("RATIO", "LT", "1"), "0", "0").met(), "ratio with zero compare");
        assertNull(eval(t("RELATIVE", "GT", "-1"), "5", "0").metric(), "relative metric with zero compare");
    }

    @Test
    void absoluteHasNoMetric() {
        ThresholdResult r = eval(t("ABSOLUTE", ">", "100"), "100.5", "0");
        assertTrue(r.met(), "100.5 > 100");
        assertNull(r.metric(), "absolute metric");
    }

    @Test
    void comparisonsAreStrictForLtGt() {
        assertFalse(eval(t("RATIO", "LT", "-1"), "-5686.4826", "-5686.4826").met(), "-1.0 < -1 is false");
        assertTrue(eval(t("RATIO", "LTE", "-1"), "-5686.4826", "-5686.4826").met(), "-1.0 <= -1");
    }

    @Test
    void unknownTypeNeverMeets() {
        assertFalse(eval(t("PERCENTILE", "LT", "1"), "0", "1").met(), "unknown type");
    }

    @Test
    void unknownOperatorIsRejectedAtParse() {
        assertThrows(IllegalArgumentException.class, () -> Operator.parse("OPERATOR_INVALID"));
    }
}
