package io.alerting.rules;

import io.alerting.model.Rule;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Reads rules from a JSON file on every refresh, so editing the file updates rules live. */
public final class JsonFileRuleProvider implements RuleProvider {
    private final Path path;

    public JsonFileRuleProvider(Path path) {
        this.path = path;
    }

    @Override
    public List<Rule> fetchRules() throws Exception {
        return RulesParser.parse(Files.readString(path));
    }

    @Override
    public String toString() {
        return "file:" + path;
    }
}
