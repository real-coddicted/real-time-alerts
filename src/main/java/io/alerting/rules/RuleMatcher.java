package io.alerting.rules;

import io.alerting.model.EntityKey;
import io.alerting.model.Rule;

import java.util.Map;
import java.util.Set;

/** The single place rule-to-key matching lives. */
public final class RuleMatcher {
    private RuleMatcher() {}

    /**
     * True iff scopes are equal and every attribute constrained by the rule is present on the key with one of
     * the allowed values. Attributes the rule does not mention are wildcards.
     */
    public static boolean matches(Rule rule, EntityKey key) {
        if (!rule.scope().equals(key.scope())) return false;
        for (Map.Entry<String, Set<String>> e : rule.match().entrySet()) {
            String actual = key.attribute(e.getKey());
            if (actual == null || !e.getValue().contains(actual)) return false;
        }
        return true;
    }
}
