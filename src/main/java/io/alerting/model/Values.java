package io.alerting.model;

import java.math.BigDecimal;

/** Conversions between loosely typed record values and the decimal/string forms the engine compares. */
public final class Values {
    private Values() {}

    /** Parses a decimal; {@code null} and blank strings are zero. Throws {@link NumberFormatException} otherwise. */
    public static BigDecimal toDecimal(Object v) {
        if (v == null) return BigDecimal.ZERO;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        if (v instanceof Number) return new BigDecimal(v.toString());
        String s = v.toString().trim();
        return s.isEmpty() ? BigDecimal.ZERO : new BigDecimal(s);
    }

    /**
     * Canonical string used for equality (key attributes, rule match values, filters): numbers drop
     * trailing zeros so {@code 25}, {@code "25"} and {@code 25.0} compare equal.
     */
    public static String canonical(Object v) {
        if (v == null) return null;
        if (v instanceof Number) {
            BigDecimal d = toDecimal(v);
            return d.signum() == 0 ? "0" : d.stripTrailingZeros().toPlainString();
        }
        return v.toString();
    }

    /** The value as-is (raw), {@code ""} for null. */
    public static String plain(Object v) {
        if (v == null) return "";
        if (v instanceof BigDecimal) return ((BigDecimal) v).toPlainString();
        return v.toString();
    }

    /** Parsed then re-rendered as a plain decimal string, keeping scale ({@code "43.410"} stays {@code "43.410"}). */
    public static String normalizedDecimal(Object v) {
        return toDecimal(v).toPlainString();
    }
}
