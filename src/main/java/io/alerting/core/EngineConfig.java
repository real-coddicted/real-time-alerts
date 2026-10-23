package io.alerting.core;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;

/** Engine behavior shared by every stream. Serializable so it travels inside the DoFns. */
public final class EngineConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Duration outputInterval;
    private final boolean keepTicking;
    private final ZoneId zone;
    private final int alertIdBytes;
    private final TimeBasis timeBasis;
    private final boolean dedupeAgainstLastEmitted;
    private final boolean resolveOnRuleRemoval;
    private final Duration ruleRefreshInterval;

    private EngineConfig(Builder b) {
        if (b.outputInterval.isNegative()) throw new IllegalArgumentException("outputInterval must be >= 0");
        if (b.ruleRefreshInterval.isZero() || b.ruleRefreshInterval.isNegative()) {
            throw new IllegalArgumentException("ruleRefreshInterval must be > 0");
        }
        this.outputInterval = b.outputInterval;
        this.keepTicking = b.keepTicking;
        this.zone = b.zone;
        this.alertIdBytes = b.alertIdBytes;
        this.timeBasis = b.timeBasis;
        this.dedupeAgainstLastEmitted = b.dedupeAgainstLastEmitted;
        this.resolveOnRuleRemoval = b.resolveOnRuleRemoval;
        this.ruleRefreshInterval = b.ruleRefreshInterval;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static EngineConfig defaults() {
        return builder().build();
    }

    /** Per-key throttle: at most one evaluation per key per interval, using the latest event. */
    public Duration outputInterval() {
        return outputInterval;
    }

    /** Emit FIRED_UPDATED for active alerts even when below the fire threshold. */
    public boolean keepTicking() {
        return keepTicking;
    }

    /** Time zone defining the business day (end-of-day expiry). */
    public ZoneId zone() {
        return zone;
    }

    public int alertIdBytes() {
        return alertIdBytes;
    }

    public TimeBasis timeBasis() {
        return timeBasis;
    }

    /** Compare new values against the last emitted value instead of the value at fire time. */
    public boolean dedupeAgainstLastEmitted() {
        return dedupeAgainstLastEmitted;
    }

    /** Emit RESOLVED_RULE_REMOVED when an active alert's rule disappears or stops matching. */
    public boolean resolveOnRuleRemoval() {
        return resolveOnRuleRemoval;
    }

    public Duration ruleRefreshInterval() {
        return ruleRefreshInterval;
    }

    public static final class Builder {
        private Duration outputInterval = Duration.ofSeconds(1);
        private boolean keepTicking = false;
        private ZoneId zone = ZoneId.of("America/New_York");
        private int alertIdBytes = 6;
        private TimeBasis timeBasis = TimeBasis.EVENT;
        private boolean dedupeAgainstLastEmitted = false;
        private boolean resolveOnRuleRemoval = true;
        private Duration ruleRefreshInterval = Duration.ofSeconds(30);

        public Builder outputInterval(Duration v) { outputInterval = v; return this; }
        public Builder keepTicking(boolean v) { keepTicking = v; return this; }
        public Builder zone(ZoneId v) { zone = v; return this; }
        public Builder alertIdBytes(int v) { alertIdBytes = v; return this; }
        public Builder timeBasis(TimeBasis v) { timeBasis = v; return this; }
        public Builder dedupeAgainstLastEmitted(boolean v) { dedupeAgainstLastEmitted = v; return this; }
        public Builder resolveOnRuleRemoval(boolean v) { resolveOnRuleRemoval = v; return this; }
        public Builder ruleRefreshInterval(Duration v) { ruleRefreshInterval = v; return this; }

        public EngineConfig build() {
            new AlertIdGenerator(alertIdBytes); // validates
            return new EngineConfig(this);
        }
    }
}
