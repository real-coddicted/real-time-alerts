package io.alerting.engine;

import io.alerting.rules.RuleCatalogHolder;
import io.alerting.rules.RuleEvaluator;
import io.alerting.sink.AlertSink;
import io.alerting.stream.StreamDefinition;

import java.util.Map;

/** Shared, thread-safe collaborators of all shards. */
record EngineContext(EngineConfig config, RuleCatalogHolder rules, RuleEvaluator evaluator, AlertIdGenerator ids,
                     AlertSink sink, EngineMetrics metrics, Map<String, StreamDefinition<?>> streams) {

    StreamDefinition<?> stream(String name) {
        return streams.get(name);
    }
}
