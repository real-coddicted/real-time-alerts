package io.alerting.config;

import io.alerting.engine.EndOfDay;
import io.alerting.json.JsonAccess;
import io.alerting.model.EntityKey;
import io.alerting.model.Metric;
import io.alerting.model.NameValuePair;
import io.alerting.model.Values;
import io.alerting.stream.StreamDefinition;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds a {@link StreamDefinition} over JSON records from a declarative spec, so new sources and alert
 * types need no code:
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
 */
public final class ConfigStreams {
    private ConfigStreams() {}

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static StreamDefinition<Map<String, Object>> compile(Map<String, Object> spec) {
        String name = JsonAccess.requireString(spec, "name");
        try {
            StreamDefinition.Builder<Map<String, Object>> b =
                    (StreamDefinition.Builder) StreamDefinition.builder(name, JsonAccess.requireString(spec, "source")).as(Map.class);

            List<Object> steps = JsonAccess.getList(spec, "steps");
            if (steps != null) {
                for (Object step : steps) b = applyStep(b, JsonAccess.asMap(step, "step"));
            }

            b = b.key(compileKey(JsonAccess.requireMap(spec, "key")));
            if (spec.get("timestamp") != null) {
                Function<Map<String, Object>, Object> ts = Expressions.compile(spec.get("timestamp"));
                b = b.timestamp(r -> toEpochMillis(ts.apply(r)));
            }
            Map<String, Object> subjects = JsonAccess.requireMap(spec, "subjects");
            for (Map.Entry<String, Object> e : subjects.entrySet()) {
                b = b.subject(e.getKey(), compileMetric(JsonAccess.asMap(e.getValue(), "subject " + e.getKey())));
            }
            return b.build();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("stream '" + name + "': " + e.getMessage(), e);
        }
    }

    private static StreamDefinition.Builder<Map<String, Object>> applyStep(StreamDefinition.Builder<Map<String, Object>> b,
                                                                          Map<String, Object> step) {
        if (step.containsKey("filter")) {
            return b.filter(Predicates.compile(step.get("filter")));
        }
        if (step.containsKey("explode")) {
            Map<String, Object> ex = JsonAccess.asMap(step.get("explode"), "explode");
            Function<Map<String, Object>, Object> path = Expressions.compile(JsonAccess.requireString(ex, "path"));
            String as = JsonAccess.requireString(ex, "as");
            return b.flatMap(r -> explode(r, path.apply(r), as));
        }
        if (step.containsKey("unwrap")) {
            Function<Map<String, Object>, Object> path = Expressions.compile(JsonAccess.require(step, "unwrap"));
            return b.flatMap(r -> {
                Object v = path.apply(r);
                return v instanceof Map<?, ?> ? List.of(JsonAccess.asMap(v, "unwrapped value")) : List.<Map<String, Object>>of();
            });
        }
        throw new IllegalArgumentException("unknown step: " + step.keySet());
    }

    private static List<Map<String, Object>> explode(Map<String, Object> record, Object collection, String as) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (collection instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("key", e.getKey());
                entry.put("value", e.getValue());
                out.add(with(record, as, entry));
            }
        } else if (collection instanceof List<?> l) {
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

    /** yyyymmdd int, "yyyymmdd", or ISO "yyyy-mm-dd"; null → 0 (engine derives the date from event time). */
    static int toDate(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return new BigDecimal(n.toString()).intValueExact();
        String s = v.toString().trim();
        if (s.isEmpty()) return 0;
        if (s.contains("-")) return EndOfDay.toInt(LocalDate.parse(s));
        return Integer.parseInt(s);
    }

    /** Epoch millis number/string, or ISO-8601 instant. */
    static Long toEpochMillis(Object v) {
        if (v == null) return null;
        if (v instanceof Number n) return new BigDecimal(n.toString()).longValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        if (s.chars().allMatch(Character::isDigit)) return Long.parseLong(s);
        return Instant.parse(s).toEpochMilli();
    }
}
