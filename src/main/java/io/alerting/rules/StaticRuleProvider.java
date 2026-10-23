package io.alerting.rules;

import io.alerting.model.Rule;

import java.util.List;

/** In-memory rules; {@link #set} swaps them (picked up on the next refresh). */
public final class StaticRuleProvider implements RuleProvider {
    private volatile List<Rule> rules;

    public StaticRuleProvider(List<Rule> rules) {
        this.rules = List.copyOf(rules);
    }

    public void set(List<Rule> newRules) {
        this.rules = List.copyOf(newRules);
    }

    @Override
    public List<Rule> fetchRules() {
        return rules;
    }
}
