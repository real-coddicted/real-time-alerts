package io.alerting.rules;

import io.alerting.engine.EngineMetrics;
import io.alerting.model.Rule;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Holds the current {@link RuleCatalog}. A failed refresh keeps the last good catalog (the original pipeline
 * replaced it with an empty one, silently suppressing updates and resolves).
 */
public final class RuleCatalogHolder {
    private static final System.Logger LOG = System.getLogger(RuleCatalogHolder.class.getName());

    private final RuleProvider provider;
    private final EngineMetrics metrics;
    private final CountDownLatch loaded = new CountDownLatch(1);
    private volatile RuleCatalog current;
    private long nextVersion = 1;

    public RuleCatalogHolder(RuleProvider provider, EngineMetrics metrics) {
        this.provider = provider;
        this.metrics = metrics;
    }

    /** @return null until the first successful load. */
    public RuleCatalog current() {
        return current;
    }

    public synchronized boolean refresh() {
        try {
            List<Rule> rules = provider.fetchRules();
            RuleCatalog next = RuleCatalog.of(nextVersion, rules);
            if (!next.sameRulesAs(current)) {
                nextVersion++;
                current = next;
                LOG.log(System.Logger.Level.INFO, "loaded " + next.size() + " active rules (catalog v" + next.version() + ") from " + provider);
            }
            metrics.ruleRefreshes.increment();
            loaded.countDown();
            return true;
        } catch (Exception e) {
            metrics.ruleRefreshFailures.increment();
            LOG.log(System.Logger.Level.ERROR, "rule refresh from " + provider + " failed; keeping last good catalog", e);
            return false;
        }
    }

    public boolean awaitLoaded(Duration timeout) throws InterruptedException {
        return loaded.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
