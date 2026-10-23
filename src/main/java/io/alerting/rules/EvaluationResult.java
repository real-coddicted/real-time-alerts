package io.alerting.rules;

import io.alerting.model.AlertValue;

/** Outcome of evaluating one rule against one event. When several flags are set: resolved, then expired, then fired. */
public record EvaluationResult(boolean fired, boolean resolved, boolean expired, AlertValue value) {
    public static final EvaluationResult NONE = new EvaluationResult(false, false, false, null);

    static EvaluationResult fired(AlertValue v) {
        return new EvaluationResult(true, false, false, v);
    }

    static EvaluationResult resolved(AlertValue v) {
        return new EvaluationResult(false, true, false, v);
    }

    static EvaluationResult expired(AlertValue v) {
        return new EvaluationResult(false, false, true, v);
    }
}
