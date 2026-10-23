package io.alerting.pipeline;

import io.alerting.config.PipelineConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Launcher (spec 8.1 without Spring). Arguments are Beam/Flink pipeline options plus {@code --config};
 * {@code ${VAR}} references in the config are resolved from the environment.
 *
 * <pre>
 * # embedded Flink, for local runs:
 * java -jar real-time-alerts-all.jar --config=examples/risk/pipeline.json --flinkMaster=[local] --waitUntilFinish=true
 * # self-hosted Flink cluster (the jar's main class is submitted by the Flink client/operator):
 * flink run -d real-time-alerts-all.jar --config=/opt/flink/usrlib/pipeline.json --checkpointingInterval=60000
 * </pre>
 * The runner defaults to {@code FlinkRunner}; pass {@code --runner=DirectRunner} to run without Flink.
 */
public final class AlertingPipelineMain {
    private static final Logger LOG = LoggerFactory.getLogger(AlertingPipelineMain.class);

    private AlertingPipelineMain() {}

    public static void main(String[] args) throws Exception {
        AlertingOptions options = PipelineOptionsFactory.fromArgs(withDefaults(args))
                .withoutStrictParsing()
                .withValidation()
                .as(AlertingOptions.class);
        options.setStreaming(true);

        PipelineConfig config = PipelineConfig.load(options.getConfig(), Map.of());
        LOG.info("starting alerting pipeline from {} with runner {}", options.getConfig(), options.getRunner().getSimpleName());
        Pipeline pipeline = Pipeline.create(options);
        AlertingPipeline.build(pipeline, config);
        PipelineResult result = pipeline.run();
        if (options.isWaitUntilFinish()) result.waitUntilFinish();
    }

    static String[] withDefaults(String[] args) {
        List<String> all = new ArrayList<>(Arrays.asList(args));
        if (!hasOption(all, "runner")) all.add("--runner=FlinkRunner");
        return all.toArray(new String[0]);
    }

    private static boolean hasOption(List<String> args, String name) {
        for (String a : args) {
            if (a.equals("--" + name) || a.startsWith("--" + name + "=")) return true;
        }
        return false;
    }
}
