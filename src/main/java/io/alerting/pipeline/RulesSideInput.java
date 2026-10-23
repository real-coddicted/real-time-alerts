package io.alerting.pipeline;

import io.alerting.json.Json;
import io.alerting.model.Rule;
import io.alerting.rules.RuleCatalog;
import io.alerting.rules.RuleProvider;
import io.alerting.rules.RuleProviders;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The rules side input (spec section 4): a tick every refresh interval fetches the full rule set and
 * broadcasts it as a {@link RuleCatalog} singleton to every worker.
 *
 * <p>Two fixes over the spec: a failed fetch emits nothing, so the side input keeps the last good catalog
 * instead of becoming empty (K6); and panes are reduced to the newest catalog before {@code View.asSingleton},
 * so a backlog of ticks can never put two elements in one pane and fail the job (K18). Until the first
 * successful fetch the runner holds main-input elements that read the side input.
 */
public final class RulesSideInput extends PTransform<PBegin, PCollectionView<RuleCatalog>> {
    private static final long serialVersionUID = 1L;

    private final String rulesSpecJson;
    private final java.time.Duration refreshInterval;

    public RulesSideInput(Map<String, Object> rulesSpec, java.time.Duration refreshInterval) {
        RuleProviders.fromConfig(rulesSpec); // validate eagerly at submission time
        this.rulesSpecJson = Json.write(rulesSpec);
        this.refreshInterval = refreshInterval;
    }

    @Override
    public PCollectionView<RuleCatalog> expand(PBegin input) {
        PCollection<RuleCatalog> catalogs = input
                .apply("Rules refresh ticks", GenerateSequence.from(0).withRate(1, Duration.millis(refreshInterval.toMillis())))
                .apply("Rules global window", Window.<Long>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .discardingFiredPanes())
                .apply("Fetch rules", ParDo.of(new FetchRulesDoFn(rulesSpecJson)))
                .setCoder(SerializableCoder.of(RuleCatalog.class));
        return catalogs
                .apply("Newest catalog per pane", Combine.globally(new NewestCatalog()).withoutDefaults())
                .apply("Rules as singleton", View.asSingleton());
    }

    /** Polls the configured {@link RuleProvider}; emits a catalog only on success. */
    public static final class FetchRulesDoFn extends DoFn<Long, RuleCatalog> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(FetchRulesDoFn.class);

        private final String rulesSpecJson;
        private transient RuleProvider provider;
        private transient Counter refreshes;
        private transient Counter failures;
        private transient Distribution ruleCount;

        public FetchRulesDoFn(String rulesSpecJson) {
            this.rulesSpecJson = rulesSpecJson;
        }

        @Setup
        public void setup() {
            provider = RuleProviders.fromConfig(Json.parseObject(rulesSpecJson));
            refreshes = Metrics.counter("alerting.rules", "refreshes");
            failures = Metrics.counter("alerting.rules", "refresh_failures");
            ruleCount = Metrics.distribution("alerting.rules", "active_rules");
        }

        @ProcessElement
        public void process(OutputReceiver<RuleCatalog> out) {
            try {
                List<Rule> rules = provider.fetchRules();
                RuleCatalog catalog = RuleCatalog.of(System.currentTimeMillis(), rules);
                out.output(catalog);
                refreshes.inc();
                ruleCount.update(catalog.size());
                LOG.info("loaded {} active rules from {}", catalog.size(), provider);
            } catch (Exception e) {
                failures.inc();
                LOG.error("rule refresh from {} failed; keeping last good catalog", provider, e);
            }
        }
    }

    /** Keeps the most recently fetched catalog. */
    public static final class NewestCatalog extends Combine.BinaryCombineFn<RuleCatalog> {
        private static final long serialVersionUID = 1L;

        @Override
        public RuleCatalog apply(RuleCatalog left, RuleCatalog right) {
            return right.version() >= left.version() ? right : left;
        }
    }
}
