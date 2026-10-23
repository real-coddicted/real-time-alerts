package io.alerting.threshold;

import io.alerting.model.Threshold;

import java.math.BigDecimal;

/** {@code value <op> threshold}. Returns no metric. */
public final class AbsoluteThresholdEvaluator implements ThresholdEvaluator {
    @Override
    public boolean canEvaluate(String type) {
        return "ABSOLUTE".equals(type);
    }

    @Override
    public ThresholdResult evaluate(Threshold threshold, BigDecimal value, BigDecimal compare) {
        return new ThresholdResult(threshold.operator().test(value, threshold.value()), null);
    }
}
