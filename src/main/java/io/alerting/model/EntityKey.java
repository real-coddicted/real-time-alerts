package io.alerting.model;

import io.alerting.json.JsonAccess;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The object an alert is about (the spec's {@code RuleObjectKey}), in domain-neutral form: a scope, a
 * business date (yyyymmdd) and an open set of attributes. Finance might use {@code unit_id}/{@code margin_type};
 * security might use {@code user}/{@code tenant}. All alert state is held per (stream, EntityKey).
 * Attributes with null values are omitted ("unset").
 */
public final class EntityKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String scope;
    private final int date;
    private final SortedMap<String, String> attributes;

    public EntityKey(String scope, int date, Map<String, String> attributes) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.date = date;
        TreeMap<String, String> copy = new TreeMap<>();
        if (attributes != null) {
            attributes.forEach((k, v) -> {
                if (v != null) copy.put(k, v);
            });
        }
        this.attributes = Collections.unmodifiableSortedMap(copy);
    }

    /** Builds a key, canonicalizing attribute values (numbers lose trailing zeros, nulls are dropped). */
    public static EntityKey of(String scope, int date, Map<String, ?> attributes) {
        Map<String, String> m = new TreeMap<>();
        attributes.forEach((k, v) -> {
            String c = Values.canonical(v);
            if (c != null) m.put(k, c);
        });
        return new EntityKey(scope, date, m);
    }

    public String scope() {
        return scope;
    }

    public int date() {
        return date;
    }

    public SortedMap<String, String> attributes() {
        return attributes;
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

    public static EntityKey fromJson(Object json) {
        Map<String, Object> m = JsonAccess.asMap(json, "entity key");
        Map<String, Object> attrs = JsonAccess.getMap(m, "attributes");
        return of(JsonAccess.requireString(m, "scope"), JsonAccess.getInt(m, "date", 0), attrs == null ? Map.of() : attrs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntityKey)) return false;
        EntityKey that = (EntityKey) o;
        return date == that.date && scope.equals(that.scope) && attributes.equals(that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, date, attributes);
    }

    @Override
    public String toString() {
        return canonical();
    }
}
