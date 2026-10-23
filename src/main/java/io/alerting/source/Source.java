package io.alerting.source;

/**
 * A named producer of raw records. {@link #run} is called on a dedicated thread and should return when the
 * source is exhausted or {@link SourceContext#isRunning()} turns false. Implement this to plug in Kafka,
 * Kinesis, a socket, etc.
 */
public interface Source {
    String name();

    void run(SourceContext ctx) throws Exception;

    default void stop() {}
}
