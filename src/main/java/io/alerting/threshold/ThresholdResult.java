package io.alerting.threshold;

import java.math.BigDecimal;

/** @param metric the computed metric (relative/ratio), or null when the evaluator computes none. */
public record ThresholdResult(boolean met, BigDecimal metric) {
    public static final ThresholdResult NOT_MET = new ThresholdResult(false, null);
}
