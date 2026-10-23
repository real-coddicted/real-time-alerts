package io.alerting.core;

import io.alerting.model.Alert;
import io.alerting.model.AlertState;
import io.alerting.model.EntityKey;
import io.alerting.model.Metric;
import io.alerting.model.Rule;
import io.alerting.rules.EvaluationResult;
import io.alerting.rules.RuleEvaluator;
import io.alerting.threshold.ChainedThresholdEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Per-(stream, key) alert transitions (spec 5.3 / 5.4 / 5.5), free of any runtime: the Beam DoFn loads the
 * key's active alerts into a map, calls this, and writes the map back.
 *
 * <pre>
 * none   + fired        → FIRED (new id)                     ACTIVE + resolved        → RESOLVED_AUTOMATICALLY
 * none   + not fired    → nothing                            ACTIVE + expired         → RESOLVED_EXPIRED
 * ACTIVE + fired        → FIRED_UPDATED                      ACTIVE + nothing/dedupe  → nothing
 * ACTIVE + rule gone    → RESOLVED_RULE_REMOVED (opt-out)    ACTIVE + end of day      → RESOLVED_EXPIRED (expireAll)
 * </pre>
 */
public final class AlertStateMachine implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertStateMachine.class);

    private final EngineConfig config;
    private final RuleEvaluator evaluator;
    private final AlertIdGenerator ids;

    public AlertStateMachine(EngineConfig config) {
        this(config, new RuleEvaluator(ChainedThresholdEvaluator.standard()));
    }

    public AlertStateMachine(EngineConfig config, RuleEvaluator evaluator) {
        this.config = config;
        this.evaluator = evaluator;
        this.ids = new AlertIdGenerator(config.alertIdBytes());
    }

    public EngineConfig config() {
        return config;
    }

    /** What one evaluation of a key produced. */
    public static final class Outcome {
        private final List<Alert> emitted;
        private final boolean newlyFired;
        private final int errors;

        Outcome(List<Alert> emitted, boolean newlyFired, int errors) {
            this.emitted = emitted;
            this.newlyFired = newlyFired;
            this.errors = errors;
        }

        public List<Alert> emitted() {
            return emitted;
        }

        /** True when at least one new alert fired (the caller arms the end-of-day timer). */
        public boolean newlyFired() {
            return newlyFired;
        }

        /** Rules whose evaluation threw (bad event value, unsupported subject); they are skipped. */
        public int errors() {
            return errors;
        }
    }

    /**
     * Evaluates the latest event of a burst against the rules matched to the key.
     *
     * @param active  active alerts by rule id; updated in place
     * @param metrics extracts the metric for a rule subject from the event; may throw
     * @param now     time the transition is stamped with ({@link TimeBasis})
     * @param firedAt wall clock, used only to keep alert ids unique
     */
    public Outcome evaluate(String stream, EntityKey key, List<Rule> matched, Map<String, Alert> active,
                            Function<String, Metric> metrics, long now, long firedAt) {
        List<Alert> out = new ArrayList<>();
        if (config.resolveOnRuleRemoval() && !active.isEmpty()) {
            Set<String> ids = new HashSet<>();
            for (Rule r : matched) ids.add(r.id());
            Iterator<Map.Entry<String, Alert>> it = active.entrySet().iterator();
            while (it.hasNext()) {
                Alert a = it.next().getValue();
                if (!ids.contains(a.rule().id())) {
                    it.remove();
                    out.add(a.with(AlertState.RESOLVED_RULE_REMOVED, now, a.rule(), a.value()));
                }
            }
        }

        boolean newlyFired = false;
        int errors = 0;
        for (Rule rule : matched) {
            Alert current = active.get(rule.id());
            long sinceFired = current != null ? now - current.createdAt() : 0;
            EvaluationResult r;
            try {
                Metric metric = metrics.apply(rule.subject());
                r = evaluator.evaluate(metric, rule, sinceFired, current == null ? null : current.value(), config.keepTicking());
            } catch (RuntimeException e) {
                errors++;
                LOG.warn("evaluation failed: stream={} key={} rule={}: {}", stream, key.canonical(), rule.id(), e.toString());
                continue;
            }

            if (current != null) {
                if (r.resolved()) {
                    active.remove(rule.id());
                    out.add(current.with(AlertState.RESOLVED_AUTOMATICALLY, now, rule, r.value()));
                } else if (r.expired()) {
                    active.remove(rule.id());
                    out.add(current.with(AlertState.RESOLVED_EXPIRED, now, rule, current.value()));
                } else if (r.fired()) {
                    out.add(current.with(AlertState.FIRED_UPDATED, now, rule, r.value()));
                    // By default the stored alert keeps its fire-time value: dedupe/breach are relative to the fire.
                    active.put(rule.id(), config.dedupeAgainstLastEmitted()
                            ? current.with(AlertState.FIRED, now, rule, r.value())
                            : current.with(AlertState.FIRED, current.updatedAt(), rule, current.value()));
                }
            } else if (r.fired()) {
                String id = ids.generate(stream, rule, key, now, firedAt);
                Alert alert = new Alert(id, stream, rule, key, AlertState.FIRED, now, now, r.value());
                active.put(rule.id(), alert);
                out.add(alert);
                newlyFired = true;
            }
        }
        return new Outcome(out, newlyFired, errors);
    }

    /** End of business day: every active alert becomes RESOLVED_EXPIRED and the map is cleared. */
    public List<Alert> expireAll(Map<String, Alert> active, long now) {
        List<Alert> out = new ArrayList<>();
        for (Alert a : active.values()) out.add(a.with(AlertState.RESOLVED_EXPIRED, now, a.rule(), a.value()));
        active.clear();
        return out;
    }
}
