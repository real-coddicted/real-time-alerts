package io.alerting.config;

import io.alerting.json.JsonAccess;
import io.alerting.model.Values;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Record predicates:
 * <ul>
 *   <li>{@code {"all": [p...]}}, {@code {"any": [p...]}}, {@code {"not": p}}</li>
 *   <li>{@code {"field": expr, "op": OP, "value": v}} or {@code "values": [...]} with OP one of
 *       {@code eq ne in notIn containsAny lt lte gt gte exists missing}</li>
 * </ul>
 * Equality uses canonical values ({@code 25 == "25"}); ordering ops compare as decimals (missing → 0).
 */
public final class Predicates {
    private Predicates() {}

    public static Predicate<Map<String, Object>> compile(Object spec) {
        Map<String, Object> m = JsonAccess.asMap(spec, "predicate");
        if (m.containsKey("all")) return combine(m.get("all"), true);
        if (m.containsKey("any")) return combine(m.get("any"), false);
        if (m.containsKey("not")) return compile(m.get("not")).negate();

        Function<Map<String, Object>, Object> field = Expressions.compile(JsonAccess.require(m, "field"));
        String op = JsonAccess.requireString(m, "op").toLowerCase(Locale.ROOT);
        return switch (op) {
            case "exists" -> r -> field.apply(r) != null;
            case "missing" -> r -> field.apply(r) == null;
            case "eq" -> {
                String v = Values.canonical(JsonAccess.require(m, "value"));
                yield r -> v.equals(Values.canonical(field.apply(r)));
            }
            case "ne" -> {
                String v = Values.canonical(JsonAccess.require(m, "value"));
                yield r -> !v.equals(Values.canonical(field.apply(r)));
            }
            case "in" -> {
                Set<String> vs = valueSet(m);
                yield r -> vs.contains(Values.canonical(field.apply(r)));
            }
            case "notin" -> {
                Set<String> vs = valueSet(m);
                yield r -> !vs.contains(Values.canonical(field.apply(r)));
            }
            case "containsany" -> {
                Set<String> vs = valueSet(m);
                yield r -> {
                    Object v = field.apply(r);
                    if (v instanceof List<?> l) return l.stream().anyMatch(x -> vs.contains(Values.canonical(x)));
                    return v != null && vs.contains(Values.canonical(v));
                };
            }
            case "lt", "lte", "gt", "gte" -> {
                var threshold = Values.toDecimal(JsonAccess.require(m, "value"));
                yield r -> {
                    int c = Values.toDecimal(field.apply(r)).compareTo(threshold);
                    return switch (op) {
                        case "lt" -> c < 0;
                        case "lte" -> c <= 0;
                        case "gt" -> c > 0;
                        default -> c >= 0;
                    };
                };
            }
            default -> throw new IllegalArgumentException("unknown predicate op: " + op);
        };
    }

    private static Predicate<Map<String, Object>> combine(Object spec, boolean all) {
        if (!(spec instanceof List<?> list)) throw new IllegalArgumentException((all ? "all" : "any") + " must be an array");
        List<Predicate<Map<String, Object>>> ps = new ArrayList<>();
        for (Object o : list) ps.add(compile(o));
        return all ? r -> ps.stream().allMatch(p -> p.test(r)) : r -> ps.stream().anyMatch(p -> p.test(r));
    }

    private static Set<String> valueSet(Map<String, Object> m) {
        List<Object> values = JsonAccess.getList(m, "values");
        if (values == null) throw new IllegalArgumentException("predicate requires 'values'");
        Set<String> out = new HashSet<>();
        values.forEach(v -> out.add(Values.canonical(v)));
        return out;
    }
}
