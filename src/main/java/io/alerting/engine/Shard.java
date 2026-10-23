package io.alerting.engine;

import io.alerting.model.Alert;
import io.alerting.model.AlertState;
import io.alerting.model.Metric;
import io.alerting.model.Rule;
import io.alerting.rules.EvaluationResult;
import io.alerting.rules.RuleCatalog;
import io.alerting.stream.StreamDefinition;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * One state partition and the per-(stream, key) alert state machine (spec section 5). All methods except
 * {@link #size()} run on {@link #scheduler}'s thread.
 */
final class Shard {
    private static final System.Logger LOG = System.getLogger(Shard.class.getName());

    final Scheduler scheduler;
    final Semaphore permits;
    private final EngineContext ctx;
    private final Map<StateKey, KeyState> states = new ConcurrentHashMap<>();
    private long timerSeq;

    Shard(Scheduler scheduler, EngineContext ctx) {
        this.scheduler = scheduler;
        this.ctx = ctx;
        this.permits = new Semaphore(ctx.config().maxPendingPerShard());
    }

    int size() {
        return states.size();
    }

    /** Conflate: remember the latest event; arm the throttle timer on the first event of a burst. */
    void onEvent(StateKey sk, Object event, long timestamp) {
        KeyState st = states.computeIfAbsent(sk, k -> new KeyState());
        st.latestEvent = event;
        if (!st.outputArmed) {
            st.outputArmed = true;
            st.burstTimestamp = timestamp;
            armOutputTimer(sk, st);
        }
    }

    private void armOutputTimer(StateKey sk, KeyState st) {
        long gen = ++timerSeq;
        st.outputGeneration = gen;
        st.outputTimer = scheduler.schedule(() -> onOutputTimer(sk, gen), ctx.config().outputInterval().toMillis());
    }

    void onOutputTimer(StateKey sk, long generation) {
        KeyState st = states.get(sk);
        if (st == null || !st.outputArmed || st.outputGeneration != generation) return;

        RuleCatalog catalog = ctx.rules().current();
        if (catalog == null) {
            // Rules not loaded yet: hold the burst and retry, like a side input that has not produced a pane.
            armOutputTimer(sk, st);
            return;
        }
        st.outputArmed = false;
        st.outputTimer = null;
        Object event = st.latestEvent;
        st.latestEvent = null;

        StreamDefinition<?> stream = ctx.stream(sk.stream());
        long now = ctx.config().timeBasis() == TimeBasis.EVENT ? st.burstTimestamp : scheduler.now();

        if (st.matchedCatalogVersion != catalog.version()) {
            st.matchedRules = catalog.match(sk.key(), stream.subjects());
            st.matchedCatalogVersion = catalog.version();
        }
        if (ctx.config().resolveOnRuleRemoval()) resolveOrphans(st, now);
        for (Rule rule : st.matchedRules) evaluate(sk, st, stream, rule, event, now);

        if (st.activeAlerts.isEmpty()) release(sk, st);
    }

    private void resolveOrphans(KeyState st, long now) {
        if (st.activeAlerts.isEmpty()) return;
        Set<String> matched = new HashSet<>();
        for (Rule r : st.matchedRules) matched.add(r.id());
        Iterator<Map.Entry<String, Alert>> it = st.activeAlerts.entrySet().iterator();
        while (it.hasNext()) {
            Alert a = it.next().getValue();
            if (!matched.contains(a.rule().id())) {
                it.remove();
                emit(a.with(AlertState.RESOLVED_RULE_REMOVED, now, a.rule(), a.value()));
            }
        }
    }

    private void evaluate(StateKey sk, KeyState st, StreamDefinition<?> stream, Rule rule, Object event, long now) {
        Alert active = st.activeAlerts.get(rule.id());
        long sinceFired = active != null ? now - active.createdAt() : 0;
        EvaluationResult r;
        try {
            Metric metric = stream.metric(rule.subject(), event);
            r = ctx.evaluator().evaluate(metric, rule, sinceFired, active == null ? null : active.value(), ctx.config().keepTicking());
            ctx.metrics().evaluations.increment();
        } catch (RuntimeException e) {
            // A bad event or rule fails only this evaluation, never the engine.
            ctx.metrics().evaluationErrors.increment();
            LOG.log(System.Logger.Level.WARNING, "evaluation failed: stream=" + sk.stream() + " key=" + sk.key().canonical()
                    + " rule=" + rule.id() + ": " + e);
            return;
        }

        if (active != null) {
            if (r.resolved()) {
                st.activeAlerts.remove(rule.id());
                emit(active.with(AlertState.RESOLVED_AUTOMATICALLY, now, rule, r.value()));
            } else if (r.expired()) {
                st.activeAlerts.remove(rule.id());
                emit(active.with(AlertState.RESOLVED_EXPIRED, now, rule, active.value()));
            } else if (r.fired()) {
                Alert updated = active.with(AlertState.FIRED_UPDATED, now, rule, r.value());
                emit(updated);
                // By default the stored alert keeps its fire-time value (dedupe/breach are relative to the fire).
                st.activeAlerts.put(rule.id(), ctx.config().dedupeAgainstLastEmitted()
                        ? updated.with(AlertState.FIRED, now, rule, r.value())
                        : active.with(AlertState.FIRED, active.updatedAt(), rule, active.value()));
            }
        } else if (r.fired()) {
            String id = ctx.ids().generate(sk.stream(), rule, sk.key(), now, scheduler.now());
            Alert alert = new Alert(id, sk.stream(), rule, sk.key(), AlertState.FIRED, now, now, r.value());
            st.activeAlerts.put(rule.id(), alert);
            armExpiry(sk, st);
            emit(alert);
        }
    }

    private void armExpiry(StateKey sk, KeyState st) {
        if (st.expiryTimer != null) return;
        long delay = EndOfDay.millisUntilEndOfDay(sk.key().date(), scheduler.now(), ctx.config().zone());
        long gen = ++timerSeq;
        st.expiryGeneration = gen;
        st.expiryTimer = scheduler.schedule(() -> onExpiryTimer(sk, gen), delay);
    }

    /** End of business day: expire every active alert on the key and drop all of its state. */
    void onExpiryTimer(StateKey sk, long generation) {
        KeyState st = states.get(sk);
        if (st == null || st.expiryGeneration != generation) return;
        long now = scheduler.now();
        for (Alert a : st.activeAlerts.values()) {
            emit(a.with(AlertState.RESOLVED_EXPIRED, now, a.rule(), a.value()));
        }
        st.activeAlerts.clear();
        st.expiryTimer = null;
        release(sk, st);
    }

    /** Keys without active alerts hold no state at all, so state is bounded by active alerts plus in-flight bursts. */
    private void release(StateKey sk, KeyState st) {
        if (st.outputTimer != null) st.outputTimer.cancel();
        if (st.expiryTimer != null) st.expiryTimer.cancel();
        states.remove(sk, st);
    }

    private void emit(Alert alert) {
        try {
            ctx.sink().emit(alert);
        } catch (RuntimeException e) {
            ctx.metrics().sinkErrors.increment();
            LOG.log(System.Logger.Level.ERROR, "sink failed for alert " + alert.alertId() + " " + alert.state(), e);
            return;
        }
        ctx.metrics().alertEmitted(alert.state());
    }

}
