package io.alerting.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Value attached to an alert. {@code breachValue} is the subject value when the alert first fired and is
 * carried over unchanged on updates; {@code valueToCompare}/{@code metricValue} are set only when the fire
 * threshold computed a metric (relative, ratio).
 */
public record AlertValue(NameValuePair breachValue, NameValuePair subjectValue, NameValuePair valueToCompare, String metricValue) {
    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("breach_value", breachValue == null ? null : breachValue.toJson());
        m.put("subject_value", subjectValue == null ? null : subjectValue.toJson());
        if (valueToCompare != null) m.put("value_to_compare", valueToCompare.toJson());
        if (metricValue != null) m.put("metric_value", metricValue);
        return m;
    }
}
