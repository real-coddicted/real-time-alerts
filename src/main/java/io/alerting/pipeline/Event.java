package io.alerting.pipeline;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * A domain-neutral event flowing through the pipeline: the decoded record as JSON plus its timestamp
 * (epoch ms). Out of a source the timestamp is the Kafka record time; after a stream prepares it, it is
 * the event time used for alert stamping.
 */
public final class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String json;
    private final long timestamp;

    public Event(String json, long timestamp) {
        this.json = Objects.requireNonNull(json, "json");
        this.timestamp = timestamp;
    }

    public String json() {
        return json;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Event && json.equals(((Event) o).json) && timestamp == ((Event) o).timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(json, timestamp);
    }

    @Override
    public String toString() {
        return "Event{" + timestamp + " " + json + "}";
    }

    public static final class EventCoder extends AtomicCoder<Event> {
        private static final long serialVersionUID = 1L;
        private static final EventCoder INSTANCE = new EventCoder();

        public static EventCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(Event value, OutputStream out) throws IOException {
            StringUtf8Coder.of().encode(value.json, out);
            VarLongCoder.of().encode(value.timestamp, out);
        }

        @Override
        public Event decode(InputStream in) throws IOException {
            return new Event(StringUtf8Coder.of().decode(in), VarLongCoder.of().decode(in));
        }
    }
}
