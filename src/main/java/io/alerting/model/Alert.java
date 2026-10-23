package io.alerting.model;

import io.alerting.json.JsonAccess;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An alert transition record. All transitions of one alert share {@code alertId}; the sink keys on
 * {@code (alertId, date)} so they stay ordered on one partition.
 */
public final class Alert implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String alertId;
    private final String stream;
    private final Rule rule;
    private final EntityKey key;
    private final AlertState state;
    private final long createdAt;
    private final long updatedAt;
    private final AlertValue value;

    public Alert(String alertId, String stream, Rule rule, EntityKey key, AlertState state,
                 long createdAt, long updatedAt, AlertValue value) {
        this.alertId = Objects.requireNonNull(alertId, "alertId");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.rule = Objects.requireNonNull(rule, "rule");
        this.key = Objects.requireNonNull(key, "key");
        this.state = Objects.requireNonNull(state, "state");
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.value = value;
    }

    public String alertId() {
        return alertId;
    }

    public String stream() {
        return stream;
    }

    public Rule rule() {
        return rule;
    }

    public EntityKey key() {
        return key;
    }

    public AlertState state() {
        return state;
    }

    public long createdAt() {
        return createdAt;
    }

    public long updatedAt() {
        return updatedAt;
    }

    public AlertValue value() {
        return value;
    }

    public int date() {
        return key.date();
    }

    public Alert with(AlertState newState, long newUpdatedAt, Rule newRule, AlertValue newValue) {
        return new Alert(alertId, stream, newRule, key, newState, createdAt, newUpdatedAt, newValue);
    }

    /** Sink key: {@code {"alert_id": ..., "date": ...}}. */
    public Map<String, Object> keyJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("alert_id", alertId);
        m.put("date", key.date());
        return m;
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

    public static Alert fromJson(Map<String, Object> m) {
        return new Alert(
                JsonAccess.requireString(m, "alert_id"),
                JsonAccess.requireString(m, "stream"),
                Rule.fromJson(JsonAccess.requireMap(m, "rule")),
                EntityKey.fromJson(JsonAccess.require(m, "rule_object")),
                AlertState.valueOf(JsonAccess.requireString(m, "state")),
                JsonAccess.getLong(m, "created_at", 0),
                JsonAccess.getLong(m, "updated_at", 0),
                AlertValue.fromJson(m.get("value")));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Alert)) return false;
        Alert a = (Alert) o;
        return createdAt == a.createdAt && updatedAt == a.updatedAt && alertId.equals(a.alertId)
                && stream.equals(a.stream) && rule.equals(a.rule) && key.equals(a.key) && state == a.state
                && Objects.equals(value, a.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId, stream, rule, key, state, createdAt, updatedAt, value);
    }

    @Override
    public String toString() {
        return "Alert{" + alertId + " " + state + " " + rule.id() + " " + key.canonical() + "}";
    }
}
