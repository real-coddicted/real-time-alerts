package io.alerting.threshold;

import java.math.BigDecimal;

/** {@code metric} is the computed metric (relative/ratio), or null when the evaluator computes none. */
public final class ThresholdResult {
    public static final ThresholdResult NOT_MET = new ThresholdResult(false, null);

    private final boolean met;
    private final BigDecimal metric;

    public ThresholdResult(boolean met, BigDecimal metric) {
        this.met = met;
        this.metric = metric;
    }

    public boolean met() {
        return met;
    }

    public BigDecimal metric() {
        return metric;
    }
}
