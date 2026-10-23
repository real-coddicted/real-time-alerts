package io.alerting.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Loads text or bytes from {@code classpath:/x} (bundled in the job jar) or a file path. */
public final class Resources {
    private static final String CLASSPATH = "classpath:";

    private Resources() {}

    public static String read(String location) throws IOException {
        return new String(readBytes(location), StandardCharsets.UTF_8);
    }

    public static byte[] readBytes(String location) throws IOException {
        if (location.startsWith(CLASSPATH)) {
            String name = location.substring(CLASSPATH.length());
            if (name.startsWith("/")) name = name.substring(1);
            try (InputStream in = Resources.class.getClassLoader().getResourceAsStream(name)) {
                if (in == null) throw new IOException("classpath resource not found: " + name);
                return in.readAllBytes();
            }
        }
        return Files.readAllBytes(Path.of(location));
    }

    /** Resolves {@code relative} against the directory of {@code base} unless it is absolute or {@code classpath:}. */
    public static String resolve(String base, String relative) {
        if (relative.startsWith(CLASSPATH) || Path.of(relative).isAbsolute() || base == null) return relative;
        if (base.startsWith(CLASSPATH)) {
            String dir = base.substring(0, base.lastIndexOf('/') + 1);
            return dir.isEmpty() || dir.equals(CLASSPATH) ? CLASSPATH + "/" + relative : dir + relative;
        }
        Path parent = Path.of(base).toAbsolutePath().getParent();
        return parent.resolve(relative).toString();
    }
}
