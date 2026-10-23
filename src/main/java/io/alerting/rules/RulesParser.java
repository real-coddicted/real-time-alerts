package io.alerting.rules;

import io.alerting.json.Json;
import io.alerting.json.JsonAccess;
import io.alerting.model.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses rules from JSON: either an array of rules or {@code {"rules": [...]}}. Invalid rules (unknown
 * operator, missing fields, non-numeric threshold) are logged and skipped so one bad rule cannot poison the job.
 */
public final class RulesParser {
    private static final Logger LOG = LoggerFactory.getLogger(RulesParser.class);

    private RulesParser() {}

    public static List<Rule> parse(String json) {
        return parse(Json.parse(json));
    }

    public static List<Rule> parse(Object json) {
        List<?> items;
        if (json instanceof List) {
            items = (List<?>) json;
        } else if (json instanceof Map) {
            List<Object> l = JsonAccess.getList(JsonAccess.asMap(json, "rules document"), "rules");
            items = l == null ? List.of() : l;
        } else {
            throw new IllegalArgumentException("rules document must be an array or an object with 'rules'");
        }
        List<Rule> out = new ArrayList<>(items.size());
        for (Object item : items) {
            try {
                out.add(Rule.fromJson(JsonAccess.asMap(item, "rule")));
            } catch (RuntimeException e) {
                LOG.warn("skipping invalid rule {}: {}", Json.write(item), e.getMessage());
            }
        }
        return out;
    }
}
