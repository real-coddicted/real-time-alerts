package io.alerting.pipeline;

import io.alerting.config.PipelineConfig;
import io.alerting.config.Resources;
import io.alerting.json.Json;
import io.alerting.json.JsonAccess;
import io.alerting.rules.RuleCatalog;
import io.alerting.rules.RuleProviders;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builds DirectRunner pipelines that drive {@link AlertingPipeline#alerts} with a single TestStream. */
final class PipelineTestSupport {
    private PipelineTestSupport() {}

    static Pipeline pipeline() {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        options.as(StreamingOptions.class).setStreaming(true);
        return Pipeline.create(options);
    }

    /** A static rules side input, fetched once from the config's provider. */
    static PCollectionView<RuleCatalog> staticRules(Pipeline p, PipelineConfig config) throws Exception {
        RuleCatalog catalog = RuleCatalog.of(1, RuleProviders.fromConfig(config.rules()).fetchRules());
        return p.apply("Static rules", Create.of(catalog).withCoder(SerializableCoder.of(RuleCatalog.class)))
                .apply("Rules view", View.asSingleton());
    }

    static TestStream.Builder<KV<String, Event>> stream() {
        return TestStream.create(KvCoder.of(StringUtf8Coder.of(), Event.EventCoder.of()));
    }

    /** Splits one ordered TestStream of (sourceName, event) into the per-source inputs of the pipeline. */
    static Map<String, PCollection<Event>> sources(PipelineConfig config, PCollection<KV<String, Event>> input) {
        Map<String, PCollection<Event>> out = new LinkedHashMap<>();
        for (Map<String, Object> s : config.sources()) {
            String name = JsonAccess.requireString(s, "name");
            out.put(name, input.apply("Only " + name, ParDo.of(new SelectSource(name))).setCoder(Event.EventCoder.of()));
        }
        return out;
    }

    static final class SelectSource extends DoFn<KV<String, Event>, Event> {
        private static final long serialVersionUID = 1L;
        private final String name;

        SelectSource(String name) {
            this.name = name;
        }

        @ProcessElement
        public void process(@Element KV<String, Event> e, OutputReceiver<Event> out) {
            if (name.equals(e.getKey())) out.output(e.getValue());
        }
    }

    /** One line of a scenario.jsonl replay file. */
    static final class Step {
        final String name;
        final String source;
        final String recordJson;
        final List<String> expectedNew;

        Step(String name, String source, String recordJson, List<String> expectedNew) {
            this.name = name;
            this.source = source;
            this.recordJson = recordJson;
            this.expectedNew = expectedNew;
        }
    }

    @SuppressWarnings("unchecked")
    static List<Step> scenario(String path) throws Exception {
        List<Step> steps = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(path))) {
            if (line.isBlank() || line.startsWith("//")) continue;
            Map<String, Object> env = Json.parseObject(line);
            List<String> expected = new ArrayList<>();
            for (Object pair : (List<Object>) JsonAccess.requireMap(env, "expect").get("new")) {
                List<Object> p = (List<Object>) pair;
                expected.add(p.get(0) + " " + p.get(1));
            }
            steps.add(new Step(JsonAccess.requireString(env, "step"), JsonAccess.requireString(env, "source"),
                    Json.write(env.get("record")), expected));
        }
        return steps;
    }

    static PipelineConfig load(String path, String outputIntervalMs) throws Exception {
        Map<String, String> vars = new HashMap<>();
        vars.put("OUTPUT_INTERVAL_MS", outputIntervalMs);
        return PipelineConfig.load(path, vars);
    }

    static String read(String path) throws Exception {
        return Resources.read(path);
    }
}
