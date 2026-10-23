package io.alerting.model;

import io.alerting.json.JsonAccess;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** A threshold: {@code type} selects the evaluator (ABSOLUTE, RELATIVE, RATIO or a custom one). */
public record Threshold(String type, Operator operator, BigDecimal value) {

    public Threshold {
        type = normalizeType(Objects.requireNonNull(type, "type"));
        Objects.requireNonNull(operator, "operator");
        Objects.requireNonNull(value, "value");
    }

    public static String normalizeType(String type) {
        String t = type.trim().toUpperCase(Locale.ROOT);
        return t.startsWith("THRESHOLD_TYPE_") ? t.substring("THRESHOLD_TYPE_".length()) : t;
    }

    public static Threshold fromJson(Map<String, Object> m) {
        String type = JsonAccess.requireString(m, "type");
        Operator op = Operator.parse(JsonAccess.requireString(m, "operator"));
        BigDecimal value;
        try {
            value = Values.toDecimal(JsonAccess.require(m, "value"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("threshold value is not a number: " + m.get("value"));
        }
        return new Threshold(type, op, value);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("operator", operator.name());
        m.put("value", value.toPlainString());
        return m;
    }
}
