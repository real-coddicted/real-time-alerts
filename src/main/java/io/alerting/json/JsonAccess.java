package io.alerting.json;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/** Typed accessors over parsed JSON maps, with consistent error messages for config and rule parsing. */
public final class JsonAccess {
    private JsonAccess() {}

    public static Object require(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) throw new IllegalArgumentException("missing required field '" + key + "'");
        return v;
    }

    public static String requireString(Map<String, Object> m, String key) {
        String s = getString(m, key, null);
        if (s == null || s.isBlank()) throw new IllegalArgumentException("missing required field '" + key + "'");
        return s.trim();
    }

    public static String getString(Map<String, Object> m, String key, String def) {
        Object v = m.get(key);
        if (v == null) return def;
        if (v instanceof BigDecimal d) return d.toPlainString();
        return v.toString();
    }

    public static long getLong(Map<String, Object> m, String key, long def) {
        Object v = m.get(key);
        if (v == null) return def;
        if (v instanceof Number n) return new BigDecimal(n.toString()).longValueExact();
        return Long.parseLong(v.toString().trim());
    }

    public static int getInt(Map<String, Object> m, String key, int def) {
        return Math.toIntExact(getLong(m, key, def));
    }

    public static boolean getBoolean(Map<String, Object> m, String key, boolean def) {
        Object v = m.get(key);
        if (v == null) return def;
        if (v instanceof Boolean b) return b;
        return Boolean.parseBoolean(v.toString().trim());
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getMap(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) return null;
        if (!(v instanceof Map)) throw new IllegalArgumentException("field '" + key + "' must be an object");
        return (Map<String, Object>) v;
    }

    public static Map<String, Object> requireMap(Map<String, Object> m, String key) {
        Map<String, Object> v = getMap(m, key);
        if (v == null) throw new IllegalArgumentException("missing required object '" + key + "'");
        return v;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> getList(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) return null;
        if (!(v instanceof List)) throw new IllegalArgumentException("field '" + key + "' must be an array");
        return (List<Object>) v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asMap(Object v, String what) {
        if (!(v instanceof Map)) throw new IllegalArgumentException(what + " must be an object");
        return (Map<String, Object>) v;
    }
}
