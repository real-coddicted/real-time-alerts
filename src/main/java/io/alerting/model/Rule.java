package io.alerting.model;

import io.alerting.json.JsonAccess;

import java.io.Serializable;
import java.util.ArrayList;
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
 * <p>{@code subject} names the metric a stream must be able to extract (e.g. TOTAL_PNL, FAILED_LOGINS);
 * {@code scope} must equal the key's scope. {@code match} maps key attribute names to the allowed values;
 * an attribute absent from {@code match} is a wildcard. Alert state is tracked per rule {@code id}, so editing
 * a rule (new {@code version}) keeps the active alert instead of orphaning it.
 */
public final class Rule implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Fire threshold; {@code expiryMs} is the time after firing at which a non-breaching alert expires (negative = never). */
    public static final class Fire implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Threshold threshold;
        private final long expiryMs;

        public Fire(Threshold threshold, long expiryMs) {
            this.threshold = Objects.requireNonNull(threshold, "fire threshold");
            this.expiryMs = expiryMs;
        }

        public Threshold threshold() {
            return threshold;
        }

        public long expiryMs() {
            return expiryMs;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Fire && threshold.equals(((Fire) o).threshold) && expiryMs == ((Fire) o).expiryMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(threshold, expiryMs);
        }
    }

    /** Resolve threshold; an alert cannot auto-resolve until {@code minTimeToResolveMs} after it fired. */
    public static final class Resolve implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Threshold threshold;
        private final long minTimeToResolveMs;

        public Resolve(Threshold threshold, long minTimeToResolveMs) {
            this.threshold = Objects.requireNonNull(threshold, "resolve threshold");
            this.minTimeToResolveMs = minTimeToResolveMs;
        }

        public Threshold threshold() {
            return threshold;
        }

        public long minTimeToResolveMs() {
            return minTimeToResolveMs;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Resolve && threshold.equals(((Resolve) o).threshold)
                    && minTimeToResolveMs == ((Resolve) o).minTimeToResolveMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(threshold, minTimeToResolveMs);
        }
    }

    private final String id;
    private final long version;
    private final String subject;
    private final String scope;
    private final Map<String, Set<String>> match;
    private final Fire fire;
    private final Resolve resolve;
    private final String severity;
    private final boolean active;

    public Rule(String id, long version, String subject, String scope, Map<String, Set<String>> match,
                Fire fire, Resolve resolve, String severity, boolean active) {
        this.id = Objects.requireNonNull(id, "id");
        this.version = version;
        this.subject = Objects.requireNonNull(subject, "subject");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.fire = Objects.requireNonNull(fire, "fire");
        this.resolve = resolve;
        this.severity = severity;
        this.active = active;
        TreeMap<String, Set<String>> m = new TreeMap<>();
        if (match != null) match.forEach((k, v) -> m.put(k, Collections.unmodifiableSet(new TreeSet<>(v))));
        this.match = Collections.unmodifiableMap(m);
    }

    public String id() {
        return id;
    }

    public long version() {
        return version;
    }

    public String subject() {
        return subject;
    }

    public String scope() {
        return scope;
    }

    public Map<String, Set<String>> match() {
        return match;
    }

    public Fire fire() {
        return fire;
    }

    /** Null when the rule has no resolve threshold. */
    public Resolve resolve() {
        return resolve;
    }

    public String severity() {
        return severity;
    }

    public boolean active() {
        return active;
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
                if (v instanceof List) {
                    ((List<?>) v).forEach(x -> values.add(Values.canonical(x)));
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
        match.forEach((k, v) -> mm.put(k, v.size() == 1 ? v.iterator().next() : new ArrayList<>(v)));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Rule)) return false;
        Rule r = (Rule) o;
        return version == r.version && active == r.active && id.equals(r.id) && subject.equals(r.subject)
                && scope.equals(r.scope) && match.equals(r.match) && fire.equals(r.fire)
                && Objects.equals(resolve, r.resolve) && Objects.equals(severity, r.severity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, subject, scope, match, fire, resolve, severity, active);
    }

    @Override
    public String toString() {
        return "Rule{" + id + " v" + version + " " + subject + "/" + scope + "}";
    }
}
