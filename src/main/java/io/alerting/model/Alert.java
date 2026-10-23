package io.alerting.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An alert transition record. All transitions of one alert share {@code alertId}; downstream partitioning
 * should key on {@code (alertId, date)} so they stay ordered.
 */
public record Alert(String alertId, String stream, Rule rule, EntityKey key, AlertState state,
                    long createdAt, long updatedAt, AlertValue value) {

    public int date() {
        return key.date();
    }

    public Alert with(AlertState newState, long newUpdatedAt, Rule newRule, AlertValue newValue) {
        return new Alert(alertId, stream, newRule, key, newState, createdAt, newUpdatedAt, newValue);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("alert_id", alertId);
        m.put("date", key.date());
        m.put("state", state.name());
        m.put("stream", stream);
        m.put("rule", rule.toJson());
        m.put("rule_object", key.toJson());
        m.put("created_at", createdAt);
        m.put("updated_at", updatedAt);
        m.put("value", value == null ? null : value.toJson());
        return m;
    }
}
