package io.alerting.source;

public interface SourceContext {
    /** Emits a record under this source's name. {@code timestamp} (epoch ms) is optional. */
    void emit(Object record, Long timestamp);

    /** Emits a record under another source name (for multiplexed inputs). */
    void emitTo(String sourceName, Object record, Long timestamp);

    boolean isRunning();
}
