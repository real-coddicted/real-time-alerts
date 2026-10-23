package io.alerting.threshold;

import io.alerting.model.Threshold;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Delegates to the first evaluator that accepts the threshold type; unknown types are never met. */
public final class ChainedThresholdEvaluator implements ThresholdEvaluator {
    private final List<ThresholdEvaluator> delegates;

    public ChainedThresholdEvaluator(List<? extends ThresholdEvaluator> delegates) {
        this.delegates = List.copyOf(delegates);
    }

    /** ABSOLUTE, RELATIVE and RATIO, plus any extra evaluators (checked first). */
    public static ChainedThresholdEvaluator standard(ThresholdEvaluator... extra) {
        List<ThresholdEvaluator> all = new ArrayList<>(Arrays.asList(extra));
        all.add(new AbsoluteThresholdEvaluator());
        all.add(new RelativeThresholdEvaluator());
        all.add(new RatioThresholdEvaluator());
        return new ChainedThresholdEvaluator(all);
    }

    @Override
    public boolean canEvaluate(String type) {
        return delegates.stream().anyMatch(d -> d.canEvaluate(type));
    }

    @Override
    public ThresholdResult evaluate(Threshold threshold, BigDecimal value, BigDecimal compare) {
        for (ThresholdEvaluator d : delegates) {
            if (d.canEvaluate(threshold.type())) return d.evaluate(threshold, value, compare);
        }
        return ThresholdResult.NOT_MET;
    }
}
