package io.alerting.sink;

import io.alerting.json.Json;
import io.alerting.model.Alert;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/** Writes each alert as one JSON line. */
public final class JsonLinesSink implements AlertSink {
    private final Writer out;
    private final boolean closeStream;

    public JsonLinesSink(OutputStream out, boolean closeStream) {
        this.out = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        this.closeStream = closeStream;
    }

    @Override
    public synchronized void emit(Alert alert) {
        try {
            out.write(Json.write(alert.toJson()));
            out.write('\n');
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        out.flush();
        if (closeStream) out.close();
    }
}
