package io.alerting.pipeline;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.alerting.config.Resources;
import io.alerting.json.JsonAccess;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Turns a raw Kafka value into the JSON record streams operate on. This is what makes the pipeline
 * domain-neutral: any topic can be onboarded with config alone.
 * <ul>
 *   <li>{@code "format": "json"} — the value is UTF-8 JSON</li>
 *   <li>{@code "format": "protobuf", "descriptorSet": "classpath:/x.desc", "messageType": "pkg.Msg"} — the value is
 *       a binary protobuf message, decoded dynamically from a {@code FileDescriptorSet}
 *       ({@code protoc --include_imports --descriptor_set_out=x.desc ...}) and printed as JSON with proto field
 *       names (enums as names, int64 as strings, maps as objects)</li>
 * </ul>
 */
public interface RecordDecoder {
    String decode(byte[] value) throws IOException;

    static RecordDecoder fromConfig(Map<String, Object> source) throws IOException {
        String format = JsonAccess.getString(source, "format", "json").toLowerCase(Locale.ROOT);
        switch (format) {
            case "json":
                return value -> new String(value, StandardCharsets.UTF_8);
            case "protobuf":
                return protobuf(Resources.readBytes(JsonAccess.requireString(source, "descriptorSet")),
                        JsonAccess.requireString(source, "messageType"));
            default:
                throw new IllegalArgumentException("unknown source format: " + format);
        }
    }

    static RecordDecoder protobuf(byte[] descriptorSet, String messageType) throws IOException {
        Descriptors.Descriptor descriptor = findMessage(FileDescriptorSet.parseFrom(descriptorSet), messageType);
        JsonFormat.Printer printer = JsonFormat.printer().preservingProtoFieldNames().omittingInsignificantWhitespace();
        return value -> {
            try {
                return printer.print(DynamicMessage.parseFrom(descriptor, value));
            } catch (InvalidProtocolBufferException e) {
                throw new IOException("cannot decode " + messageType + ": " + e.getMessage(), e);
            }
        };
    }

    private static Descriptors.Descriptor findMessage(FileDescriptorSet set, String messageType) throws IOException {
        Map<String, FileDescriptorProto> protos = new HashMap<>();
        for (FileDescriptorProto p : set.getFileList()) protos.put(p.getName(), p);
        Map<String, Descriptors.FileDescriptor> built = new HashMap<>();
        try {
            for (FileDescriptorProto p : set.getFileList()) {
                Descriptors.FileDescriptor fd = build(p.getName(), protos, built);
                Descriptors.Descriptor d = fd.findMessageTypeByName(simpleName(fd, messageType));
                if (d != null && d.getFullName().equals(messageType)) return d;
                if (fd.getPackage().isEmpty() || messageType.startsWith(fd.getPackage() + ".")) {
                    Descriptors.Descriptor nested = findNested(fd, messageType);
                    if (nested != null) return nested;
                }
            }
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IOException("invalid descriptor set: " + e.getMessage(), e);
        }
        throw new IOException("message type " + messageType + " not found in descriptor set");
    }

    private static String simpleName(Descriptors.FileDescriptor fd, String fullName) {
        String pkg = fd.getPackage();
        return !pkg.isEmpty() && fullName.startsWith(pkg + ".") ? fullName.substring(pkg.length() + 1) : fullName;
    }

    private static Descriptors.Descriptor findNested(Descriptors.FileDescriptor fd, String fullName) {
        for (Descriptors.Descriptor d : fd.getMessageTypes()) {
            Descriptors.Descriptor hit = findNested(d, fullName);
            if (hit != null) return hit;
        }
        return null;
    }

    private static Descriptors.Descriptor findNested(Descriptors.Descriptor d, String fullName) {
        if (d.getFullName().equals(fullName)) return d;
        for (Descriptors.Descriptor n : d.getNestedTypes()) {
            Descriptors.Descriptor hit = findNested(n, fullName);
            if (hit != null) return hit;
        }
        return null;
    }

    private static Descriptors.FileDescriptor build(String name, Map<String, FileDescriptorProto> protos,
                                                    Map<String, Descriptors.FileDescriptor> built)
            throws Descriptors.DescriptorValidationException, IOException {
        Descriptors.FileDescriptor done = built.get(name);
        if (done != null) return done;
        FileDescriptorProto proto = protos.get(name);
        if (proto == null) throw new IOException("descriptor set is missing dependency " + name + " (use --include_imports)");
        Descriptors.FileDescriptor[] deps = new Descriptors.FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < deps.length; i++) deps[i] = build(proto.getDependency(i), protos, built);
        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(proto, deps);
        built.put(name, fd);
        return fd;
    }
}
