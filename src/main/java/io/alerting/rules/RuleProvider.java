package io.alerting.rules;

import io.alerting.model.Rule;

import java.util.List;

/** Source of the full rule set. Polled periodically; throwing keeps the last good catalog in place. */
@FunctionalInterface
public interface RuleProvider {
    List<Rule> fetchRules() throws Exception;
}
