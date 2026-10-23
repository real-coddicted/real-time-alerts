package io.alerting.pipeline;

import io.alerting.json.Json;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Kafka record → {@link Event}. Records with a null value are dropped (spec 1.1); undecodable ones are counted and dropped. */
public final class DecodeDoFn extends DoFn<KafkaRecord<byte[], byte[]>, Event> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DecodeDoFn.class);

    private final String sourceName;
    private final String sourceSpecJson;
    private transient RecordDecoder decoder;
    private transient Counter dropped;
    private transient Counter errors;

    public DecodeDoFn(String sourceName, Map<String, Object> sourceSpec) {
        this.sourceName = sourceName;
        this.sourceSpecJson = Json.write(sourceSpec);
    }

    @Setup
    public void setup() throws Exception {
        decoder = RecordDecoder.fromConfig(Json.parseObject(sourceSpecJson));
        dropped = Metrics.counter("alerting.source." + sourceName, "null_values");
        errors = Metrics.counter("alerting.source." + sourceName, "decode_errors");
    }

    @ProcessElement
    public void process(@Element KafkaRecord<byte[], byte[]> record, OutputReceiver<Event> out) {
        byte[] value = record.getKV().getValue();
        if (value == null) {
            dropped.inc();
            LOG.warn("dropping record with null value from {} partition {} offset {}", record.getTopic(), record.getPartition(), record.getOffset());
            return;
        }
        try {
            out.output(new Event(decoder.decode(value), record.getTimestamp()));
        } catch (Exception e) {
            errors.inc();
            LOG.warn("cannot decode record from {} partition {} offset {}: {}", record.getTopic(), record.getPartition(), record.getOffset(), e.toString());
        }
    }
}
