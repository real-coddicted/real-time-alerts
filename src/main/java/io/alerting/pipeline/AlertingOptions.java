package io.alerting.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface AlertingOptions extends PipelineOptions, StreamingOptions {
    @Description("Pipeline config JSON: a file path or classpath:/path bundled in the job jar")
    @Validation.Required
    String getConfig();

    void setConfig(String value);

    @Description("Block until the pipeline terminates (local runs); detached by default for cluster submission")
    @Default.Boolean(false)
    boolean isWaitUntilFinish();

    void setWaitUntilFinish(boolean value);
}
