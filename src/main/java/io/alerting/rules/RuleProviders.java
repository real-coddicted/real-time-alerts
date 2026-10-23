package io.alerting.rules;

import io.alerting.json.JsonAccess;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@link RuleProvider} from the {@code rules} config section:
 * <ul>
 *   <li>{@code {"type": "file", "path": "classpath:/rules.json" | "/etc/rules.json"}}</li>
 *   <li>{@code {"type": "http", "url": ..., "headers": {...}, "pageSize": 1000, "connectTimeoutMs": 60000,
 *       "readTimeoutMs": 120000, "maxAttempts": 5, "initialBackoffMs": 2000, "maxBackoffMs": 30000}}</li>
 *   <li>{@code {"type": "inline", "rules": [...]}}</li>
 * </ul>
 */
public final class RuleProviders {
    private RuleProviders() {}

    public static RuleProvider fromConfig(Map<String, Object> m) {
        String type = JsonAccess.requireString(m, "type");
        switch (type) {
            case "file":
                return new JsonFileRuleProvider(JsonAccess.requireString(m, "path"));
            case "inline":
                List<Object> rules = JsonAccess.getList(m, "rules");
                return new StaticRuleProvider(RulesParser.parse(rules == null ? List.of() : rules));
            case "http":
                Map<String, String> headers = new LinkedHashMap<>();
                Map<String, Object> h = JsonAccess.getMap(m, "headers");
                if (h != null) h.forEach((k, v) -> headers.put(k, String.valueOf(v)));
                return new HttpRuleProvider(
                        URI.create(JsonAccess.requireString(m, "url")),
                        headers,
                        JsonAccess.getInt(m, "pageSize", 1000),
                        Duration.ofMillis(JsonAccess.getLong(m, "connectTimeoutMs", 60_000)),
                        Duration.ofMillis(JsonAccess.getLong(m, "readTimeoutMs", 120_000)),
                        JsonAccess.getInt(m, "maxAttempts", 5),
                        Duration.ofMillis(JsonAccess.getLong(m, "initialBackoffMs", 2_000)),
                        Duration.ofMillis(JsonAccess.getLong(m, "maxBackoffMs", 30_000)));
            default:
                throw new IllegalArgumentException("unknown rules type: " + type);
        }
    }
}
