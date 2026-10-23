package io.alerting.rules;

import io.alerting.config.Resources;
import io.alerting.model.Rule;

import java.util.List;

/** Reads rules from a JSON file ({@code classpath:} or filesystem path) on every refresh. */
public final class JsonFileRuleProvider implements RuleProvider {
    private final String location;

    public JsonFileRuleProvider(String location) {
        this.location = location;
    }

    @Override
    public List<Rule> fetchRules() throws Exception {
        return RulesParser.parse(Resources.read(location));
    }

    @Override
    public String toString() {
        return "file:" + location;
    }
}
