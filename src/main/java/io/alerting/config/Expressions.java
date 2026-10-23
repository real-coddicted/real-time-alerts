package io.alerting.config;

import io.alerting.json.JsonAccess;
import io.alerting.model.Values;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Value expressions evaluated against a JSON record:
 * <ul>
 *   <li>{@code "a.b.c"} — field path (map keys or list indexes); missing → null</li>
 *   <li>{@code {"field": "a.b", "default": "0"}}</li>
 *   <li>{@code {"const": x}}, or a bare number/boolean</li>
 *   <li>{@code {"min": [e1, e2, ...]}}, {@code {"max": [...]}} — decimal min/max</li>
 *   <li>{@code {"coalesce": [e1, e2, ...]}} — first non-null</li>
 * </ul>
 */
public final class Expressions {
    private Expressions() {}

    public static Function<Map<String, Object>, Object> compile(Object spec) {
        if (spec instanceof String path) {
            String[] parts = path.split("\\.");
            return r -> get(r, parts);
        }
        if (spec instanceof Number || spec instanceof Boolean) {
            return r -> spec;
        }
        if (!(spec instanceof Map)) throw new IllegalArgumentException("invalid expression: " + spec);
        Map<String, Object> m = JsonAccess.asMap(spec, "expression");
        if (m.containsKey("const")) {
            Object v = m.get("const");
            return r -> v;
        }
        if (m.containsKey("field")) {
            Function<Map<String, Object>, Object> f = compile(JsonAccess.requireString(m, "field"));
            Object def = m.get("default");
            return r -> {
                Object v = f.apply(r);
                return v == null ? def : v;
            };
        }
        if (m.containsKey("min") || m.containsKey("max")) {
            boolean min = m.containsKey("min");
            List<Function<Map<String, Object>, Object>> args = compileAll(m.get(min ? "min" : "max"));
            return r -> {
                BigDecimal best = null;
                for (var a : args) {
                    BigDecimal v = Values.toDecimal(a.apply(r));
                    if (best == null || (min ? v.compareTo(best) < 0 : v.compareTo(best) > 0)) best = v;
                }
                return best;
            };
        }
        if (m.containsKey("coalesce")) {
            List<Function<Map<String, Object>, Object>> args = compileAll(m.get("coalesce"));
            return r -> {
                for (var a : args) {
                    Object v = a.apply(r);
                    if (v != null) return v;
                }
                return null;
            };
        }
        throw new IllegalArgumentException("unknown expression: " + m.keySet());
    }

    private static List<Function<Map<String, Object>, Object>> compileAll(Object spec) {
        if (!(spec instanceof List<?> list) || list.isEmpty()) throw new IllegalArgumentException("expected a non-empty array: " + spec);
        List<Function<Map<String, Object>, Object>> out = new ArrayList<>();
        for (Object o : list) out.add(compile(o));
        return out;
    }

    static Object get(Object record, String[] path) {
        Object cur = record;
        for (String p : path) {
            if (cur instanceof Map<?, ?> m) {
                cur = m.get(p);
            } else if (cur instanceof List<?> l) {
                int i;
                try {
                    i = Integer.parseInt(p);
                } catch (NumberFormatException e) {
                    return null;
                }
                cur = i >= 0 && i < l.size() ? l.get(i) : null;
            } else {
                return null;
            }
            if (cur == null) return null;
        }
        return cur;
    }
}
