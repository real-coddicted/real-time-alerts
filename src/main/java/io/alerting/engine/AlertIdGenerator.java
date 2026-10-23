package io.alerting.engine;

import io.alerting.model.EntityKey;
import io.alerting.model.Rule;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

/**
 * {@code base64url(sha256(stream:ruleId:ruleVersion:key:createdAt:firedAt)[0..n]) + ":" + createdAt}.
 * Hashes a portable canonical form instead of protobuf text output, so ids are reproducible in any language.
 * {@code firedAt} is the engine clock at firing: with event-time stamping two separate fires can share
 * {@code createdAt} (same upstream timestamp), and must still get distinct ids.
 */
public final class AlertIdGenerator {
    private final int bytes;

    public AlertIdGenerator(int bytes) {
        if (bytes < 1 || bytes > 32) throw new IllegalArgumentException("alert id bytes must be in [1, 32]: " + bytes);
        this.bytes = bytes;
    }

    public String generate(String stream, Rule rule, EntityKey key, long createdAt, long firedAt) {
        String input = String.join(":", stream, rule.id(), Long.toString(rule.version()), key.canonical(),
                Long.toString(createdAt), Long.toString(firedAt));
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(Arrays.copyOf(digest, bytes)) + ":" + createdAt;
    }
}
