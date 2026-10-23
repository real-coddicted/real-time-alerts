package io.alerting.model;

import io.alerting.json.JsonAccess;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Value attached to an alert. {@code breachValue} is the subject value when the alert first fired and is
 * carried over unchanged on updates; {@code valueToCompare}/{@code metricValue} are set only when the fire
 * threshold computed a metric (relative, ratio).
 */
public final class AlertValue implements Serializable {
    private static final long serialVersionUID = 1L;

    private final NameValuePair breachValue;
    private final NameValuePair subjectValue;
    private final NameValuePair valueToCompare;
    private final String metricValue;

    public AlertValue(NameValuePair breachValue, NameValuePair subjectValue, NameValuePair valueToCompare, String metricValue) {
        this.breachValue = breachValue;
        this.subjectValue = subjectValue;
        this.valueToCompare = valueToCompare;
        this.metricValue = metricValue;
    }

    public NameValuePair breachValue() {
        return breachValue;
    }

    public NameValuePair subjectValue() {
        return subjectValue;
    }

    public NameValuePair valueToCompare() {
        return valueToCompare;
    }

    public String metricValue() {
        return metricValue;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("breach_value", breachValue == null ? null : breachValue.toJson());
        m.put("subject_value", subjectValue == null ? null : subjectValue.toJson());
        if (valueToCompare != null) m.put("value_to_compare", valueToCompare.toJson());
        if (metricValue != null) m.put("metric_value", metricValue);
        return m;
    }

    public static AlertValue fromJson(Object json) {
        if (json == null) return null;
        Map<String, Object> m = JsonAccess.asMap(json, "alert value");
        return new AlertValue(
                NameValuePair.fromJson(m.get("breach_value")),
                NameValuePair.fromJson(m.get("subject_value")),
                NameValuePair.fromJson(m.get("value_to_compare")),
                JsonAccess.getString(m, "metric_value", null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AlertValue)) return false;
        AlertValue that = (AlertValue) o;
        return Objects.equals(breachValue, that.breachValue) && Objects.equals(subjectValue, that.subjectValue)
                && Objects.equals(valueToCompare, that.valueToCompare) && Objects.equals(metricValue, that.metricValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(breachValue, subjectValue, valueToCompare, metricValue);
    }

    @Override
    public String toString() {
        return toJson().toString();
    }
}
