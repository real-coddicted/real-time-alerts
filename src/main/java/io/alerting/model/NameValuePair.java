package io.alerting.model;

import io.alerting.json.JsonAccess;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class NameValuePair implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String value;

    public NameValuePair(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String name() {
        return name;
    }

    public String value() {
        return value;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("value", value);
        return m;
    }

    public static NameValuePair fromJson(Object json) {
        if (json == null) return null;
        Map<String, Object> m = JsonAccess.asMap(json, "name/value pair");
        return new NameValuePair(JsonAccess.getString(m, "name", null), JsonAccess.getString(m, "value", null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NameValuePair)) return false;
        NameValuePair that = (NameValuePair) o;
        return Objects.equals(name, that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return name + "=" + value;
    }
}
