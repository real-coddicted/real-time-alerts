package io.alerting.model;

import java.math.BigDecimal;
import java.util.Locale;

public enum Operator {
    LT, GT, LTE, GTE;

    public boolean test(BigDecimal x, BigDecimal threshold) {
        int c = x.compareTo(threshold);
        switch (this) {
            case LT:
                return c < 0;
            case GT:
                return c > 0;
            case LTE:
                return c <= 0;
            default:
                return c >= 0;
        }
    }

    /** Accepts short names, symbols and the proto enum names ({@code OPERATOR_LESS_THAN}). */
    public static Operator parse(String s) {
        String n = s.trim().toUpperCase(Locale.ROOT);
        if (n.startsWith("OPERATOR_")) n = n.substring("OPERATOR_".length());
        switch (n) {
            case "LT":
            case "LESS_THAN":
            case "<":
                return LT;
            case "GT":
            case "GREATER_THAN":
            case ">":
                return GT;
            case "LTE":
            case "LESS_THAN_OR_EQUAL":
            case "<=":
                return LTE;
            case "GTE":
            case "GREATER_THAN_OR_EQUAL":
            case ">=":
                return GTE;
            default:
                throw new IllegalArgumentException("unsupported operator: " + s);
        }
    }
}
