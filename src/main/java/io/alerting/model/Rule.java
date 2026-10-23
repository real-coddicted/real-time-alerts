package io.alerting.model;

import io.alerting.json.JsonAccess;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A user defined alert rule.
 *
 * <p>{@code subject} names the metric a stream must be able to extract (e.g. TOTAL_PNL); {@code scope} must
 * equal the key's scope. {@code match} maps key attribute names to the allowed values; an attribute that is
 * absent from {@code match} is a wildcard. Alert state is tracked per rule {@code id}, so editing a rule (new
 * {@code version}) keeps the active alert instead of orphaning it.
 */
public record Rule(String id, long version, String subject, String scope, Map<String, Set<String>> match,
                   Fire fire, Resolve resolve, String severity, boolean active) {

    /** @param expiryMs time after firing at which an alert that is no longer breaching expires; negative = never. */
    public record Fire(Threshold threshold, long expiryMs) {
        public Fire {
            Objects.requireNonNull(threshold, "fire threshold");
        }
    }

    /** @param minTimeToResolveMs an alert cannot auto-resolve until this long after it fired. */
    public record Resolve(Threshold threshold, long minTimeToResolveMs) {
        public Resolve {
            Objects.requireNonNull(threshold, "resolve threshold");
        }
    }

    public Rule {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(fire, "fire");
        TreeMap<String, Set<String>> m = new TreeMap<>();
        if (match != null) match.forEach((k, v) -> m.put(k, Collections.unmodifiableSet(new TreeSet<>(v))));
        match = Collections.unmodifiableMap(m);
    }

    public static Rule fromJson(Map<String, Object> m) {
        String id = Values.canonical(JsonAccess.require(m, "id"));
        long version = JsonAccess.getLong(m, "version", 0);
        String subject = JsonAccess.requireString(m, "subject");
        String scope = JsonAccess.requireString(m, "scope");

        Map<String, Set<String>> match = new TreeMap<>();
        Map<String, Object> mm = JsonAccess.getMap(m, "match");
        if (mm != null) {
            mm.forEach((attr, v) -> {
                if (v == null) return;
                Set<String> values = new TreeSet<>();
                if (v instanceof List<?> list) {
                    list.forEach(x -> values.add(Values.canonical(x)));
                } else {
                    values.add(Values.canonical(v));
                }
                if (!values.isEmpty()) match.put(attr, values);
            });
        }

        Map<String, Object> f = JsonAccess.requireMap(m, "fire");
        Fire fire = new Fire(Threshold.fromJson(f), JsonAccess.getLong(f, "expiryMs", -1));
        Map<String, Object> r = JsonAccess.getMap(m, "resolve");
        Resolve resolve = r == null ? null : new Resolve(Threshold.fromJson(r), JsonAccess.getLong(r, "minTimeToResolveMs", 0));

        return new Rule(id, version, subject, scope, match, fire, resolve,
                JsonAccess.getString(m, "severity", null), JsonAccess.getBoolean(m, "active", true));
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("version", version);
        m.put("subject", subject);
        m.put("scope", scope);
        Map<String, Object> mm = new LinkedHashMap<>();
        match.forEach((k, v) -> mm.put(k, v.size() == 1 ? v.iterator().next() : List.copyOf(v)));
        m.put("match", mm);
        Map<String, Object> f = fire.threshold().toJson();
        f.put("expiryMs", fire.expiryMs());
        m.put("fire", f);
        if (resolve != null) {
            Map<String, Object> r = resolve.threshold().toJson();
            r.put("minTimeToResolveMs", resolve.minTimeToResolveMs());
            m.put("resolve", r);
        }
        if (severity != null) m.put("severity", severity);
        m.put("active", active);
        return m;
    }
}
