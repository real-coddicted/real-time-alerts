package io.alerting.sink;

import io.alerting.model.Alert;

/**
 * Destination for alert transitions. Called from shard threads, so implementations must be thread-safe.
 * Implement this to write to Kafka (key on {@code (alertId, date)} to keep each alert's transitions ordered).
 */
public interface AlertSink extends AutoCloseable {
    void emit(Alert alert);

    @Override
    default void close() throws java.io.IOException {}
}
