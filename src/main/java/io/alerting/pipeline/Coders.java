package io.alerting.pipeline;

import io.alerting.json.Json;
import io.alerting.model.Alert;
import io.alerting.model.EntityKey;
import io.alerting.model.Rule;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.TreeMap;

/**
 * Coders for engine types. State values are encoded as JSON (not Java serialization) so persisted Flink
 * state survives class changes; the key coder is deterministic as required for grouping.
 */
public final class Coders {
    private Coders() {}

    /** Deterministic: scope, date, then attributes in sorted order (spec's RuleObjectKeyCoder equivalent). */
    public static final class EntityKeyCoder extends AtomicCoder<EntityKey> {
        private static final long serialVersionUID = 1L;
        private static final EntityKeyCoder INSTANCE = new EntityKeyCoder();

        public static EntityKeyCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(EntityKey key, OutputStream out) throws IOException {
            StringUtf8Coder.of().encode(key.scope(), out);
            VarIntCoder.of().encode(key.date(), out);
            VarIntCoder.of().encode(key.attributes().size(), out);
            for (Map.Entry<String, String> e : key.attributes().entrySet()) {
                StringUtf8Coder.of().encode(e.getKey(), out);
                StringUtf8Coder.of().encode(e.getValue(), out);
            }
        }

        @Override
        public EntityKey decode(InputStream in) throws IOException {
            String scope = StringUtf8Coder.of().decode(in);
            int date = VarIntCoder.of().decode(in);
            int n = VarIntCoder.of().decode(in);
            Map<String, String> attrs = new TreeMap<>();
            for (int i = 0; i < n; i++) attrs.put(StringUtf8Coder.of().decode(in), StringUtf8Coder.of().decode(in));
            return new EntityKey(scope, date, attrs);
        }

        @Override
        public void verifyDeterministic() {}

        @Override
        public boolean consistentWithEquals() {
            return true;
        }
    }

    public static final class AlertCoder extends AtomicCoder<Alert> {
        private static final long serialVersionUID = 1L;
        private static final AlertCoder INSTANCE = new AlertCoder();

        public static AlertCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(Alert value, OutputStream out) throws IOException {
            StringUtf8Coder.of().encode(Json.write(value.toJson()), out);
        }

        @Override
        public Alert decode(InputStream in) throws IOException {
            return Alert.fromJson(Json.parseObject(StringUtf8Coder.of().decode(in)));
        }
    }

    public static final class RuleCoder extends AtomicCoder<Rule> {
        private static final long serialVersionUID = 1L;
        private static final RuleCoder INSTANCE = new RuleCoder();

        public static RuleCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(Rule value, OutputStream out) throws IOException {
            StringUtf8Coder.of().encode(Json.write(value.toJson()), out);
        }

        @Override
        public Rule decode(InputStream in) throws IOException {
            return Rule.fromJson(Json.parseObject(StringUtf8Coder.of().decode(in)));
        }
    }
}
