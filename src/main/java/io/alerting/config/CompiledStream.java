package io.alerting.config;

import io.alerting.core.EndOfDay;
import io.alerting.json.JsonAccess;
import io.alerting.model.EntityKey;
import io.alerting.model.Metric;
import io.alerting.model.NameValuePair;
import io.alerting.model.Values;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * One alerting stream compiled from its declarative spec. A stream is the domain-specific part of the
 * pipeline: which source it reads, how a decoded record becomes keyed items, and which rule subjects it can
 * evaluate. Everything else (rules, state machine, sink) is shared, so onboarding a new domain is config only.
 *
 * <pre>{@code
 * {
 *   "name": "regt-excess", "source": "tick-regt",
 *   "timestamp": "timestamp",
 *   "steps": [ {"filter": {...}}, {"explode": {"path": "issuers", "as": "issuer"}}, {"unwrap": "payload.value"} ],
 *   "key": { "scope": "UNIT", "date": "date",
 *            "attributes": { "unit_id": "unit_id", "margin_type": {"const": "MARGIN_TYPE_REGT"} } },
 *   "subjects": {
 *     "EXCESS": { "subject": {"name": "excess", "value": {"min": ["house_excess", "sma"]}},
 *                 "compare": {"name": "equity", "value": "equity"}, "normalize": true }
 *   }
 * }
 * }</pre>
 *
 * Steps: {@code filter} (see {@link Predicates}); {@code explode} emits one record per map entry or list
 * element, adding {@code {"key"|"index": ..., "value": ...}} under {@code as}; {@code unwrap} replaces the
 * record with a nested object (dropping the record if absent).
 *
 * <p>Instances hold lambdas and are not serializable: DoFns keep the spec JSON and compile it in {@code @Setup}.
 */
public final class CompiledStream {
    private final String name;
    private final String source;
    private final List<Function<Map<String, Object>, List<Map<String, Object>>>> steps;
    private final Function<Map<String, Object>, EntityKey> key;
    private final Function<Map<String, Object>, Object> timestamp;
    private final Map<String, Function<Map<String, Object>, Metric>> subjects;

    private CompiledStream(String name, String source, List<Function<Map<String, Object>, List<Map<String, Object>>>> steps,
                           Function<Map<String, Object>, EntityKey> key, Function<Map<String, Object>, Object> timestamp,
                           Map<String, Function<Map<String, Object>, Metric>> subjects) {
        this.name = name;
        this.source = source;
        this.steps = steps;
        this.key = key;
        this.timestamp = timestamp;
        this.subjects = subjects;
    }

    public static CompiledStream compile(Map<String, Object> spec) {
        String name = JsonAccess.requireString(spec, "name");
        try {
            List<Function<Map<String, Object>, List<Map<String, Object>>>> steps = new ArrayList<>();
            List<Object> stepSpecs = JsonAccess.getList(spec, "steps");
            if (stepSpecs != null) {
                for (Object s : stepSpecs) steps.add(compileStep(JsonAccess.asMap(s, "step")));
            }
            Map<String, Function<Map<String, Object>, Metric>> subjects = new LinkedHashMap<>();
            Map<String, Object> subjectSpecs = JsonAccess.requireMap(spec, "subjects");
            if (subjectSpecs.isEmpty()) throw new IllegalArgumentException("at least one subject is required");
            subjectSpecs.forEach((subject, s) -> subjects.put(subject, compileMetric(JsonAccess.asMap(s, "subject " + subject))));
            return new CompiledStream(
                    name,
                    JsonAccess.requireString(spec, "source"),
                    steps,
                    compileKey(JsonAccess.requireMap(spec, "key")),
                    spec.get("timestamp") == null ? null : Expressions.compile(spec.get("timestamp")),
                    Collections.unmodifiableMap(subjects));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("stream '" + name + "': " + e.getMessage(), e);
        }
    }

    public String name() {
        return name;
    }

    public String source() {
        return source;
    }

    public Set<String> subjects() {
        return subjects.keySet();
    }

    /** Runs the filter/explode/unwrap steps on a decoded source record. */
    public List<Map<String, Object>> prepare(Map<String, Object> record) {
        List<Map<String, Object>> current = List.of(record);
        for (Function<Map<String, Object>, List<Map<String, Object>>> step : steps) {
            List<Map<String, Object>> next = new ArrayList<>();
            for (Map<String, Object> r : current) next.addAll(step.apply(r));
            current = next;
            if (current.isEmpty()) break;
        }
        return current;
    }

    public EntityKey key(Map<String, Object> item) {
        return key.apply(item);
    }

    /** Event time in epoch ms, or null to fall back to the source record timestamp. */
    public Long timestamp(Map<String, Object> item) {
        return timestamp == null ? null : toEpochMillis(timestamp.apply(item));
    }

    public Metric metric(String subject, Map<String, Object> item) {
        Function<Map<String, Object>, Metric> f = subjects.get(subject);
        if (f == null) throw new IllegalArgumentException("stream " + name + " does not support subject " + subject);
        return f.apply(item);
    }

    private static Function<Map<String, Object>, List<Map<String, Object>>> compileStep(Map<String, Object> step) {
        if (step.containsKey("filter")) {
            Predicate<Map<String, Object>> p = Predicates.compile(step.get("filter"));
            return r -> p.test(r) ? List.of(r) : List.of();
        }
        if (step.containsKey("explode")) {
            Map<String, Object> ex = JsonAccess.asMap(step.get("explode"), "explode");
            Function<Map<String, Object>, Object> path = Expressions.compile(JsonAccess.requireString(ex, "path"));
            String as = JsonAccess.requireString(ex, "as");
            return r -> explode(r, path.apply(r), as);
        }
        if (step.containsKey("unwrap")) {
            Function<Map<String, Object>, Object> path = Expressions.compile(JsonAccess.require(step, "unwrap"));
            return r -> {
                Object v = path.apply(r);
                return v instanceof Map ? List.of(JsonAccess.asMap(v, "unwrapped value")) : List.of();
            };
        }
        throw new IllegalArgumentException("unknown step: " + step.keySet());
    }

    private static List<Map<String, Object>> explode(Map<String, Object> record, Object collection, String as) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (collection instanceof Map) {
            for (Map.Entry<?, ?> e : ((Map<?, ?>) collection).entrySet()) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("key", e.getKey());
                entry.put("value", e.getValue());
                out.add(with(record, as, entry));
            }
        } else if (collection instanceof List) {
            List<?> l = (List<?>) collection;
            for (int i = 0; i < l.size(); i++) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("index", BigDecimal.valueOf(i));
                entry.put("value", l.get(i));
                out.add(with(record, as, entry));
            }
        }
        return out;
    }

    private static Map<String, Object> with(Map<String, Object> record, String field, Object value) {
        Map<String, Object> copy = new LinkedHashMap<>(record);
        copy.put(field, value);
        return copy;
    }

    private static Function<Map<String, Object>, EntityKey> compileKey(Map<String, Object> spec) {
        String scope = JsonAccess.requireString(spec, "scope");
        Function<Map<String, Object>, Object> date = spec.get("date") == null ? r -> null : Expressions.compile(spec.get("date"));
        Map<String, Function<Map<String, Object>, Object>> attrs = new LinkedHashMap<>();
        Map<String, Object> attrSpec = JsonAccess.getMap(spec, "attributes");
        if (attrSpec != null) attrSpec.forEach((k, v) -> attrs.put(k, Expressions.compile(v)));
        return r -> {
            Map<String, Object> values = new LinkedHashMap<>();
            attrs.forEach((k, f) -> values.put(k, f.apply(r)));
            return EntityKey.of(scope, toDate(date.apply(r)), values);
        };
    }

    private static Function<Map<String, Object>, Metric> compileMetric(Map<String, Object> spec) {
        boolean normalize = JsonAccess.getBoolean(spec, "normalize", true);
        Map<String, Object> subj = JsonAccess.requireMap(spec, "subject");
        String subjName = JsonAccess.requireString(subj, "name");
        Function<Map<String, Object>, Object> subjValue = Expressions.compile(JsonAccess.require(subj, "value"));
        Map<String, Object> cmp = JsonAccess.getMap(spec, "compare");
        String cmpName = cmp == null ? null : JsonAccess.requireString(cmp, "name");
        Function<Map<String, Object>, Object> cmpValue = cmp == null ? null : Expressions.compile(JsonAccess.require(cmp, "value"));
        Function<Object, String> render = normalize ? Values::normalizedDecimal : Values::plain;
        return r -> new Metric(
                new NameValuePair(subjName, render.apply(subjValue.apply(r))),
                cmp == null ? null : new NameValuePair(cmpName, render.apply(cmpValue.apply(r))));
    }

    /** yyyymmdd int, "yyyymmdd", or ISO "yyyy-mm-dd"; null → 0 (the date is then derived from event time). */
    static int toDate(Object v) {
        if (v == null) return 0;
        if (v instanceof Number) return new BigDecimal(v.toString()).intValueExact();
        String s = v.toString().trim();
        if (s.isEmpty()) return 0;
        if (s.contains("-")) return EndOfDay.toInt(LocalDate.parse(s));
        return Integer.parseInt(s);
    }

    /** Epoch millis number/string, or ISO-8601 instant. */
    static Long toEpochMillis(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return new BigDecimal(v.toString()).longValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        if (s.chars().allMatch(Character::isDigit)) return Long.parseLong(s);
        return Instant.parse(s).toEpochMilli();
    }
}
