package io.alerting.pipeline;

import io.alerting.config.CompiledStream;
import io.alerting.core.EndOfDay;
import io.alerting.json.Json;
import io.alerting.model.EntityKey;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Map;

/**
 * Runs a stream's filter/explode/unwrap steps and keys each item (the spec's filters, issuer flatMap and
 * {@code KeyBy(RuleObjectKey::from*)}). Event time is the stream's {@code timestamp} field when present and
 * positive, else the source record time (spec 1.1 timestamping). A missing business date is derived from it.
 */
public final class PrepareDoFn extends DoFn<Event, KV<EntityKey, Event>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PrepareDoFn.class);

    private final String streamName;
    private final String streamSpecJson;
    private final ZoneId zone;
    private transient CompiledStream stream;
    private transient Counter keyed;
    private transient Counter errors;

    public PrepareDoFn(Map<String, Object> streamSpec, ZoneId zone) {
        this.streamSpecJson = Json.write(streamSpec);
        this.streamName = CompiledStream.compile(streamSpec).name();
        this.zone = zone;
    }

    @Setup
    public void setup() {
        stream = CompiledStream.compile(Json.parseObject(streamSpecJson));
        keyed = Metrics.counter("alerting.stream." + streamName, "events_keyed");
        errors = Metrics.counter("alerting.stream." + streamName, "prepare_errors");
    }

    @ProcessElement
    public void process(@Element Event event, OutputReceiver<KV<EntityKey, Event>> out) {
        Map<String, Object> record;
        try {
            record = Json.parseObject(event.json());
        } catch (RuntimeException e) {
            errors.inc();
            LOG.warn("stream {}: record is not a JSON object: {}", streamName, e.getMessage());
            return;
        }
        for (Map<String, Object> item : stream.prepare(record)) {
            try {
                Long extracted = stream.timestamp(item);
                long ts = extracted != null && extracted > 0 ? extracted
                        : event.timestamp() > 0 ? event.timestamp() : System.currentTimeMillis();
                EntityKey key = stream.key(item);
                if (key.date() == 0) key = key.withDate(EndOfDay.dateOf(ts, zone));
                EndOfDay.toLocalDate(key.date()); // validates
                out.output(KV.of(key, new Event(Json.write(item), ts)));
                keyed.inc();
            } catch (RuntimeException e) {
                errors.inc();
                LOG.warn("stream {}: cannot key item: {}", streamName, e.toString());
            }
        }
    }
}
