package io.alerting.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record NameValuePair(String name, String value) {
    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("value", value);
        return m;
    }
}
