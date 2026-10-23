package io.alerting.model;

import java.math.BigDecimal;
import java.util.Locale;

public enum Operator {
    LT, GT, LTE, GTE;

    public boolean test(BigDecimal x, BigDecimal threshold) {
        int c = x.compareTo(threshold);
        return switch (this) {
            case LT -> c < 0;
            case GT -> c > 0;
            case LTE -> c <= 0;
            case GTE -> c >= 0;
        };
    }

    /** Accepts short names, symbols and the proto enum names ({@code OPERATOR_LESS_THAN}). */
    public static Operator parse(String s) {
        String n = s.trim().toUpperCase(Locale.ROOT);
        if (n.startsWith("OPERATOR_")) n = n.substring("OPERATOR_".length());
        return switch (n) {
            case "LT", "LESS_THAN", "<" -> LT;
            case "GT", "GREATER_THAN", ">" -> GT;
            case "LTE", "LESS_THAN_OR_EQUAL", "<=" -> LTE;
            case "GTE", "GREATER_THAN_OR_EQUAL", ">=" -> GTE;
            default -> throw new IllegalArgumentException("unsupported operator: " + s);
        };
    }
}
