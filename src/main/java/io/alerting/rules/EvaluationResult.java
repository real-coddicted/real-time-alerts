package io.alerting.rules;

import io.alerting.model.AlertValue;

import java.util.Objects;

/** Outcome of evaluating one rule against one event. When several flags are set: resolved, then expired, then fired. */
public final class EvaluationResult {
    public static final EvaluationResult NONE = new EvaluationResult(false, false, false, null);

    private final boolean fired;
    private final boolean resolved;
    private final boolean expired;
    private final AlertValue value;

    public EvaluationResult(boolean fired, boolean resolved, boolean expired, AlertValue value) {
        this.fired = fired;
        this.resolved = resolved;
        this.expired = expired;
        this.value = value;
    }

    static EvaluationResult fired(AlertValue v) {
        return new EvaluationResult(true, false, false, v);
    }

    static EvaluationResult resolved(AlertValue v) {
        return new EvaluationResult(false, true, false, v);
    }

    static EvaluationResult expired(AlertValue v) {
        return new EvaluationResult(false, false, true, v);
    }

    public boolean fired() {
        return fired;
    }

    public boolean resolved() {
        return resolved;
    }

    public boolean expired() {
        return expired;
    }

    public AlertValue value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EvaluationResult)) return false;
        EvaluationResult r = (EvaluationResult) o;
        return fired == r.fired && resolved == r.resolved && expired == r.expired && Objects.equals(value, r.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fired, resolved, expired, value);
    }

    @Override
    public String toString() {
        return "EvaluationResult{fired=" + fired + ", resolved=" + resolved + ", expired=" + expired + ", value=" + value + "}";
    }
}
