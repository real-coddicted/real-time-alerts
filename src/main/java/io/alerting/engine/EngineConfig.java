package io.alerting.engine;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @param outputInterval           per-key throttle: at most one evaluation per key per interval, using the latest event
 * @param keepTicking              emit FIRED_UPDATED for active alerts even when below the fire threshold
 * @param zone                     time zone defining the business day (end-of-day expiry)
 * @param alertIdBytes             digest bytes in the alert id (1..32)
 * @param shards                   number of single-threaded state partitions
 * @param timeBasis                see {@link TimeBasis}
 * @param dedupeAgainstLastEmitted compare new values against the last emitted value instead of the value at fire time
 * @param resolveOnRuleRemoval     emit RESOLVED_RULE_REMOVED when an active alert's rule disappears or stops matching
 * @param maxPendingPerShard       back-pressure: submit blocks when this many events are queued on a shard
 * @param ruleRefreshInterval      delay between rule refreshes
 */
public record EngineConfig(Duration outputInterval, boolean keepTicking, ZoneId zone, int alertIdBytes, int shards,
                           TimeBasis timeBasis, boolean dedupeAgainstLastEmitted, boolean resolveOnRuleRemoval,
                           int maxPendingPerShard, Duration ruleRefreshInterval) {

    public EngineConfig {
        if (shards < 1) throw new IllegalArgumentException("shards must be >= 1");
        if (maxPendingPerShard < 1) throw new IllegalArgumentException("maxPendingPerShard must be >= 1");
        if (outputInterval.isNegative()) throw new IllegalArgumentException("outputInterval must be >= 0");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static EngineConfig defaults() {
        return builder().build();
    }

    public static final class Builder {
        private Duration outputInterval = Duration.ofSeconds(1);
        private boolean keepTicking = false;
        private ZoneId zone = ZoneId.of("America/New_York");
        private int alertIdBytes = 6;
        private int shards = Math.max(1, Runtime.getRuntime().availableProcessors());
        private TimeBasis timeBasis = TimeBasis.EVENT;
        private boolean dedupeAgainstLastEmitted = false;
        private boolean resolveOnRuleRemoval = true;
        private int maxPendingPerShard = 100_000;
        private Duration ruleRefreshInterval = Duration.ofSeconds(30);

        public Builder outputInterval(Duration v) { outputInterval = v; return this; }
        public Builder keepTicking(boolean v) { keepTicking = v; return this; }
        public Builder zone(ZoneId v) { zone = v; return this; }
        public Builder alertIdBytes(int v) { alertIdBytes = v; return this; }
        public Builder shards(int v) { shards = v; return this; }
        public Builder timeBasis(TimeBasis v) { timeBasis = v; return this; }
        public Builder dedupeAgainstLastEmitted(boolean v) { dedupeAgainstLastEmitted = v; return this; }
        public Builder resolveOnRuleRemoval(boolean v) { resolveOnRuleRemoval = v; return this; }
        public Builder maxPendingPerShard(int v) { maxPendingPerShard = v; return this; }
        public Builder ruleRefreshInterval(Duration v) { ruleRefreshInterval = v; return this; }

        public EngineConfig build() {
            return new EngineConfig(outputInterval, keepTicking, zone, alertIdBytes, shards, timeBasis,
                    dedupeAgainstLastEmitted, resolveOnRuleRemoval, maxPendingPerShard, ruleRefreshInterval);
        }
    }
}
