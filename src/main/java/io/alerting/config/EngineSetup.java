package io.alerting.config;

import io.alerting.engine.AlertEngine;
import io.alerting.engine.EngineConfig;
import io.alerting.rules.RuleProvider;
import io.alerting.sink.AlertSink;
import io.alerting.source.Source;
import io.alerting.stream.StreamDefinition;

import java.util.List;

/** Everything a config file describes; {@link #toEngine()} wires it up. Components can be swapped before building. */
public record EngineSetup(EngineConfig config, RuleProvider ruleProvider, List<Source> sources,
                          List<StreamDefinition<?>> streams, AlertSink sink) {

    public AlertEngine.Builder toBuilder() {
        return AlertEngine.builder().config(config).ruleProvider(ruleProvider).sources(sources).streams(streams).sink(sink);
    }

    public AlertEngine toEngine() {
        return toBuilder().build();
    }
}
