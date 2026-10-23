package io.alerting.pipeline;

import io.alerting.config.CompiledStream;
import io.alerting.config.PipelineConfig;
import io.alerting.core.EngineConfig;
import io.alerting.json.JsonAccess;
import io.alerting.rules.RuleCatalog;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the job graph from a {@link PipelineConfig} (spec section 2, generalized):
 *
 * <pre>
 * GenerateSequence(every refresh) ─▶ Fetch rules ─▶ View.asSingleton ─────────────┐ side input
 * Kafka topic A ─▶ Decode ─┬─▶ [stream 1] Prepare ─▶ AlertingDoFn (keyed state) ─┼─┐
 *                          └─▶ [stream 2] Prepare ─▶ AlertingDoFn ────────────────┤ │
 * Kafka topic B ─▶ Decode ───▶ [stream 3] Prepare ─▶ AlertingDoFn ────────────────┘ ├─▶ Flatten ─▶ Kafka alerts topic
 * </pre>
 *
 * Each source is read and decoded once no matter how many streams use it; every transform is named after
 * its source or stream so Flink operator ids stay stable across topology edits (spec K2).
 */
public final class AlertingPipeline {
    private AlertingPipeline() {}

    /** Full job: Kafka sources → alerting → Kafka sink. */
    public static void build(Pipeline pipeline, PipelineConfig config) {
        EngineConfig engine = config.engine();
        PCollectionView<RuleCatalog> rules = pipeline.apply("Rules", new RulesSideInput(config.rules(), engine.ruleRefreshInterval()));

        Map<String, Object> kafka = config.kafka();
        String bootstrap = JsonAccess.requireString(kafka, "bootstrapServers");
        Map<String, PCollection<Event>> sources = new LinkedHashMap<>();
        for (Map<String, Object> source : config.sources()) {
            String name = JsonAccess.requireString(source, "name");
            if (!usedByAnyStream(config, name)) continue;
            sources.put(name, pipeline
                    .apply("Read " + name, KafkaIO.<byte[], byte[]>read()
                            .withBootstrapServers(bootstrap)
                            .withTopic(JsonAccess.requireString(source, "topic"))
                            .withKeyDeserializer(ByteArrayDeserializer.class)
                            .withValueDeserializer(ByteArrayDeserializer.class)
                            .withConsumerConfigUpdates(consumerConfig(kafka, source))
                            .commitOffsetsInFinalize())
                    .apply("Decode " + name, ParDo.of(new DecodeDoFn(name, source)))
                    .setCoder(Event.EventCoder.of()));
        }

        Map<String, Object> sink = config.sink();
        alerts(config, sources, rules).apply("Write alerts", KafkaIO.<String, String>write()
                .withBootstrapServers(bootstrap)
                .withTopic(JsonAccess.requireString(sink, "topic"))
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withProducerConfigUpdates(producerConfig(kafka)));
    }

    /**
     * The transport-independent core: decoded events per source name → alert records
     * {@code KV<alertKeyJson, alertJson>}. Tests drive this directly with TestStream.
     */
    public static PCollection<KV<String, String>> alerts(PipelineConfig config, Map<String, PCollection<Event>> sources,
                                                         PCollectionView<RuleCatalog> rules) {
        EngineConfig engine = config.engine();
        List<PCollection<KV<String, String>>> outputs = new ArrayList<>();
        for (Map<String, Object> spec : config.streams()) {
            CompiledStream stream = CompiledStream.compile(spec);
            PCollection<Event> source = sources.get(stream.source());
            if (source == null) throw new IllegalArgumentException("no input for source " + stream.source());
            outputs.add(source
                    .apply(stream.name() + "/Prepare", ParDo.of(new PrepareDoFn(spec, engine.zone())))
                    .setCoder(KvCoder.of(Coders.EntityKeyCoder.of(), Event.EventCoder.of()))
                    .apply(stream.name() + "/Alerting", ParDo.of(new AlertingDoFn(spec, engine, rules)).withSideInputs(rules))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
        }
        if (outputs.isEmpty()) throw new IllegalArgumentException("no streams configured");
        return PCollectionList.of(outputs).apply("Flatten alerts", Flatten.pCollections())
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    }

    private static boolean usedByAnyStream(PipelineConfig config, String source) {
        for (Map<String, Object> s : config.streams()) {
            if (source.equals(s.get("source"))) return true;
        }
        return false;
    }

    /** Spec 1.1 consumer defaults, then {@code kafka.consumer} overrides, then per-source {@code offsetReset}. */
    static Map<String, Object> consumerConfig(Map<String, Object> kafka, Map<String, Object> source) {
        Map<String, Object> props = new HashMap<>();
        props.put("group.id", JsonAccess.getString(kafka, "groupId", "real-time-alerts"));
        props.put("isolation.level", "read_committed");
        props.put("fetch.max.bytes", 10485880);
        props.put("auto.offset.reset", "latest");
        Map<String, Object> extra = JsonAccess.getMap(kafka, "consumer");
        if (extra != null) extra.forEach((k, v) -> props.put(k, String.valueOf(v)));
        if (source.get("offsetReset") != null) props.put("auto.offset.reset", source.get("offsetReset").toString());
        return props;
    }

    /** Spec 1.2 producer defaults, then {@code kafka.producer} overrides. */
    static Map<String, Object> producerConfig(Map<String, Object> kafka) {
        Map<String, Object> props = new HashMap<>();
        props.put("compression.type", "lz4");
        props.put("batch.size", 1048576);
        props.put("linger.ms", 5);
        props.put("max.request.size", 10485880);
        Map<String, Object> extra = JsonAccess.getMap(kafka, "producer");
        if (extra != null) extra.forEach((k, v) -> props.put(k, String.valueOf(v)));
        return props;
    }
}
