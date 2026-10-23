package io.alerting.rules;

import io.alerting.model.AlertValue;
import io.alerting.model.Metric;
import io.alerting.model.NameValuePair;
import io.alerting.model.Rule;
import io.alerting.model.Values;
import io.alerting.threshold.ThresholdEvaluator;
import io.alerting.threshold.ThresholdResult;

import java.math.BigDecimal;

/**
 * The rule algorithm (spec section 6), independent of the event type: streams turn events into a
 * {@link Metric}, this decides fire / resolve / expire.
 */
public final class RuleEvaluator {
    private final ThresholdEvaluator thresholds;

    public RuleEvaluator(ThresholdEvaluator thresholds) {
        this.thresholds = thresholds;
    }

    /**
     * @param sinceFiredMs time since the active alert fired, 0 when there is none
     * @param prev         value of the active alert (as stored), null when there is none
     * @param keepTicking  report {@code fired} for an active alert even below the fire threshold
     */
    public EvaluationResult evaluate(Metric metric, Rule rule, long sinceFiredMs, AlertValue prev, boolean keepTicking) {
        NameValuePair subject = metric.subject();
        BigDecimal current = Values.toDecimal(subject.value());

        // Unchanged since the active alert's value: nothing to say (numeric compare, scale insensitive).
        if (prev != null && Values.toDecimal(prev.subjectValue().value()).compareTo(current) == 0) {
            return EvaluationResult.NONE;
        }

        NameValuePair breach = prev != null ? prev.breachValue() : subject;
        NameValuePair comparePair = metric.compare();
        BigDecimal compare = comparePair == null ? BigDecimal.ZERO : Values.toDecimal(comparePair.value());

        ThresholdResult fire = thresholds.evaluate(rule.fire().threshold(), current, compare);
        AlertValue value = fire.metric() != null
                ? new AlertValue(breach, subject, comparePair, fire.metric().toPlainString())
                : new AlertValue(breach, subject, null, null);

        if (fire.met()) return EvaluationResult.fired(value);

        if (prev != null && rule.resolve() != null) {
            Rule.Resolve resolve = rule.resolve();
            if (sinceFiredMs < resolve.minTimeToResolveMs()) {
                return new EvaluationResult(keepTicking, false, false, value);
            }
            if (thresholds.evaluate(resolve.threshold(), current, compare).met()) {
                return EvaluationResult.resolved(value);
            }
        }

        long expiry = rule.fire().expiryMs();
        if (prev != null && expiry >= 0 && sinceFiredMs >= expiry) {
            return EvaluationResult.expired(value);
        }
        return new EvaluationResult(prev != null && keepTicking, false, false, value);
    }
}
