package io.alerting.pipeline;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.alerting.json.Json;
import io.alerting.model.Alert;
import io.alerting.model.AlertState;
import io.alerting.model.AlertValue;
import io.alerting.model.EntityKey;
import io.alerting.model.NameValuePair;
import io.alerting.rules.RuleCatalog;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.alerting.rules.RuleEvaluatorTest.rule;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Coders (spec T20), the rules side input (spec 4.2 / S6) and source decoding. */
class ComponentsTest {

    @Test
    void entityKeyCoderRoundTripsAndIsDeterministic() throws Exception {
        EntityKey k1 = EntityKey.of("POSITION", 20240918, Map.of("unit_id", 105570, "instrument_id", 775616, "netting_id", "CST"));
        EntityKey k2 = EntityKey.of("UNIT", 20240918, Map.of());
        CoderProperties.coderDecodeEncodeEqual(Coders.EntityKeyCoder.of(), k1);
        CoderProperties.coderDecodeEncodeEqual(Coders.EntityKeyCoder.of(), k2);
        CoderProperties.coderDeterministic(Coders.EntityKeyCoder.of(),
                EntityKey.of("U", 1, Map.of("a", "1", "b", "2")), EntityKey.of("U", 1, Map.of("b", "2", "a", "1")));
    }

    @Test
    void alertAndEventCodersRoundTrip() throws Exception {
        Alert a = new Alert("id:1", "s",
                rule("{\"id\":\"R\",\"version\":2,\"subject\":\"S\",\"scope\":\"U\",\"match\":{\"x\":[\"1\",\"2\"]},"
                        + "\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"-0.2\",\"expiryMs\":5},"
                        + "\"resolve\":{\"type\":\"RATIO\",\"operator\":\"GT\",\"value\":\"-0.1\",\"minTimeToResolveMs\":3}}"),
                EntityKey.of("U", 20240918, Map.of("x", "1")), AlertState.FIRED, 1, 2,
                new AlertValue(new NameValuePair("p", "-30"), new NameValuePair("p", "-31"), new NameValuePair("b", "100"), "-0.31"));
        CoderProperties.coderDecodeEncodeEqual(Coders.AlertCoder.of(), a);
        CoderProperties.coderDecodeEncodeEqual(Event.EventCoder.of(), new Event("{\"a\":1}", 42L));
    }

    @Test
    void fetchRulesEmitsCatalogOnSuccessAndNothingOnFailure() {
        Pipeline p = PipelineTestSupport.pipeline();
        String good = "{\"type\":\"inline\",\"rules\":[{\"id\":\"A\",\"subject\":\"S\",\"scope\":\"U\","
                + "\"fire\":{\"type\":\"RATIO\",\"operator\":\"LT\",\"value\":\"1\"}}]}";
        String bad = "{\"type\":\"file\",\"path\":\"/does/not/exist.json\"}";
        PCollection<RuleCatalog> ok = p.apply("tick ok", Create.of(0L))
                .apply("fetch ok", ParDo.of(new RulesSideInput.FetchRulesDoFn(good)))
                .setCoder(SerializableCoder.of(RuleCatalog.class));
        PCollection<RuleCatalog> failed = p.apply("tick bad", Create.of(0L))
                .apply("fetch bad", ParDo.of(new RulesSideInput.FetchRulesDoFn(bad)))
                .setCoder(SerializableCoder.of(RuleCatalog.class));
        PAssert.that(ok).satisfies(cs -> {
            RuleCatalog c = cs.iterator().next();
            assertEquals(1, c.size());
            return null;
        });
        PAssert.that(failed).empty();
        p.run().waitUntilFinish();
    }

    @Test
    void newestCatalogWins() {
        Pipeline p = PipelineTestSupport.pipeline();
        PCollection<RuleCatalog> newest = p
                .apply(Create.of(RuleCatalog.of(5, List.of()), RuleCatalog.of(9, List.of()), RuleCatalog.of(7, List.of()))
                        .withCoder(SerializableCoder.of(RuleCatalog.class)))
                .apply(Combine.globally(new RulesSideInput.NewestCatalog()));
        PAssert.thatSingleton(newest).satisfies(c -> {
            assertEquals(9L, c.version());
            return null;
        });
        p.run().waitUntilFinish();
    }

    @Test
    void protobufSourceDecodesToJsonWithProtoFieldNames() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("auth.proto").setPackage("demo.auth").setSyntax("proto3")
                .addEnumType(EnumDescriptorProto.newBuilder().setName("Status")
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("STATUS_UNKNOWN").setNumber(0))
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("STATUS_LOCKED").setNumber(1)))
                .addMessageType(DescriptorProto.newBuilder().setName("LoginWindow")
                        .addField(field("user_name", 1, FieldDescriptorProto.Type.TYPE_STRING, null))
                        .addField(field("failure_count", 2, FieldDescriptorProto.Type.TYPE_INT64, null))
                        .addField(field("status", 3, FieldDescriptorProto.Type.TYPE_ENUM, ".demo.auth.Status")))
                .build();
        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
        Descriptors.Descriptor type = fd.findMessageTypeByName("LoginWindow");
        byte[] payload = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("user_name"), "alice")
                .setField(type.findFieldByName("failure_count"), 42L)
                .setField(type.findFieldByName("status"), fd.findEnumTypeByName("Status").findValueByName("STATUS_LOCKED"))
                .build().toByteArray();
        byte[] descriptorSet = FileDescriptorSet.newBuilder().addFile(file).build().toByteArray();

        Map<String, Object> json = Json.parseObject(RecordDecoder.protobuf(descriptorSet, "demo.auth.LoginWindow").decode(payload));
        assertEquals("alice", json.get("user_name"));
        assertEquals("42", json.get("failure_count"), "int64 is printed as a string; Values.toDecimal handles it");
        assertEquals("STATUS_LOCKED", json.get("status"));
        assertThrows(IOException.class, () -> RecordDecoder.protobuf(descriptorSet, "demo.auth.Nope"));
    }

    private static FieldDescriptorProto field(String name, int number, FieldDescriptorProto.Type type, String typeName) {
        FieldDescriptorProto.Builder b = FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        if (typeName != null) b.setTypeName(typeName);
        return b.build();
    }
}
