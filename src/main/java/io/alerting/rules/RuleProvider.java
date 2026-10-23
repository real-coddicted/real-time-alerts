package io.alerting.rules;

import io.alerting.model.Rule;

import java.util.List;

/** Source of the full rule set. Called periodically; throwing keeps the last good catalog in place. */
@FunctionalInterface
public interface RuleProvider {
    List<Rule> fetchRules() throws Exception;
}
