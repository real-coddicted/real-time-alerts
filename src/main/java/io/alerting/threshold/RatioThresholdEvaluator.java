package io.alerting.threshold;

import io.alerting.model.Threshold;

import java.math.BigDecimal;

/** {@code value / |compare| <op> threshold}; never met when compare is zero. */
public final class RatioThresholdEvaluator implements ThresholdEvaluator {
    @Override
    public boolean canEvaluate(String type) {
        return "RATIO".equals(type);
    }

    @Override
    public ThresholdResult evaluate(Threshold threshold, BigDecimal value, BigDecimal compare) {
        if (compare.signum() == 0) return ThresholdResult.NOT_MET;
        BigDecimal m = value.divide(compare.abs(), SCALE, ROUNDING);
        return new ThresholdResult(threshold.operator().test(m, threshold.value()), m);
    }
}
