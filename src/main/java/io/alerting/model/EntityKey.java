package io.alerting.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The object an alert is about (the spec's {@code RuleObjectKey}): a scope, a business date (yyyymmdd)
 * and an open set of attributes such as {@code unit_id}, {@code margin_type} or {@code instrument_id}.
 * All alert state is held per (stream, EntityKey). Attributes with null values are omitted ("unset").
 */
public record EntityKey(String scope, int date, SortedMap<String, String> attributes) {

    public EntityKey {
        Objects.requireNonNull(scope, "scope");
        TreeMap<String, String> copy = new TreeMap<>();
        if (attributes != null) {
            attributes.forEach((k, v) -> {
                if (v != null) copy.put(k, v);
            });
        }
        attributes = Collections.unmodifiableSortedMap(copy);
    }

    /** Builds a key, canonicalizing attribute values (numbers lose trailing zeros, nulls are dropped). */
    public static EntityKey of(String scope, int date, Map<String, ?> attributes) {
        TreeMap<String, String> m = new TreeMap<>();
        attributes.forEach((k, v) -> {
            String c = Values.canonical(v);
            if (c != null) m.put(k, c);
        });
        return new EntityKey(scope, date, m);
    }

    public String attribute(String name) {
        return attributes.get(name);
    }

    public EntityKey withDate(int newDate) {
        return new EntityKey(scope, newDate, attributes);
    }

    /** Stable, portable text form used for hashing alert ids and for logs. */
    public String canonical() {
        StringBuilder sb = new StringBuilder(scope).append('|').append(date).append('|');
        boolean first = true;
        for (Map.Entry<String, String> e : attributes.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("scope", scope);
        m.put("date", date);
        m.put("attributes", new LinkedHashMap<>(attributes));
        return m;
    }
}
