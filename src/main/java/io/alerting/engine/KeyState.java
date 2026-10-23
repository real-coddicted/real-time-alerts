package io.alerting.engine;

import io.alerting.model.Alert;
import io.alerting.model.Rule;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Mutable per-(stream, key) state. Only touched from the owning shard's scheduler thread. */
final class KeyState {
    /** Latest event of the current burst; only this one is evaluated. */
    Object latestEvent;
    /** Event timestamp of the first event of the current burst. */
    long burstTimestamp;
    boolean outputArmed;
    long outputGeneration;
    Scheduler.Cancellable outputTimer;
    long expiryGeneration;
    Scheduler.Cancellable expiryTimer;
    /** Active alerts by rule id. */
    final Map<String, Alert> activeAlerts = new LinkedHashMap<>();
    long matchedCatalogVersion = -1;
    List<Rule> matchedRules = List.of();
}
