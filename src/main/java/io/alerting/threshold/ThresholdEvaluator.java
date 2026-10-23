package io.alerting.threshold;

import io.alerting.model.Threshold;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/** Evaluates one threshold type. Register custom types by adding an implementation to the chain. */
public interface ThresholdEvaluator extends Serializable {
    int SCALE = 18;
    RoundingMode ROUNDING = RoundingMode.HALF_UP;

    boolean canEvaluate(String type);

    ThresholdResult evaluate(Threshold threshold, BigDecimal value, BigDecimal compare);
}
