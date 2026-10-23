package io.alerting.sink;

import io.alerting.model.Alert;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class InMemorySink implements AlertSink {
    private final List<Alert> alerts = new CopyOnWriteArrayList<>();

    @Override
    public void emit(Alert alert) {
        alerts.add(alert);
    }

    public List<Alert> alerts() {
        return List.copyOf(alerts);
    }

    public int size() {
        return alerts.size();
    }

    public void clear() {
        alerts.clear();
    }
}
