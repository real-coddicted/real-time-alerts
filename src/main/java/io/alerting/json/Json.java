package io.alerting.json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal dependency-free JSON reader/writer.
 *
 * <p>Objects become {@link LinkedHashMap}, arrays {@link ArrayList}, numbers {@link BigDecimal}
 * (scale preserved, so {@code 5983719.7500} round trips unchanged).
 */
public final class Json {
    private Json() {}

    public static Object parse(String text) {
        Parser p = new Parser(text);
        try {
            p.skipWs();
            Object v = p.readValue();
            p.skipWs();
            if (p.pos != text.length()) {
                throw p.error("trailing characters");
            }
            return v;
        } catch (StringIndexOutOfBoundsException e) {
            throw p.error("unexpected end of input");
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object v = parse(text);
        if (!(v instanceof Map)) {
            throw new IllegalArgumentException("expected a JSON object");
        }
        return (Map<String, Object>) v;
    }

    public static String write(Object value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String s) {
            writeString(sb, s);
        } else if (v instanceof BigDecimal d) {
            sb.append(d.toPlainString());
        } else if (v instanceof Number || v instanceof Boolean) {
            sb.append(v);
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                write(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) sb.append(',');
                first = false;
                write(sb, o);
            }
            sb.append(']');
        } else if (v instanceof Object[] arr) {
            write(sb, Arrays.asList(arr));
        } else if (v instanceof Enum<?> e) {
            writeString(sb, e.name());
        } else {
            writeString(sb, v.toString());
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        IllegalArgumentException error(String msg) {
            return new IllegalArgumentException("invalid JSON: " + msg + " at offset " + pos);
        }

        void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }

        Object readValue() {
            if (pos >= s.length()) throw error("unexpected end of input");
            char c = s.charAt(pos);
            return switch (c) {
                case '{' -> readObject();
                case '[' -> readArray();
                case '"' -> readString();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> readNumber();
            };
        }

        private Object literal(String word, Object value) {
            if (!s.startsWith(word, pos)) throw error("unexpected token");
            pos += word.length();
            return value;
        }

        private Map<String, Object> readObject() {
            pos++;
            Map<String, Object> map = new LinkedHashMap<>();
            skipWs();
            if (s.charAt(pos) == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWs();
                if (s.charAt(pos) != '"') throw error("expected object key");
                String key = readString();
                skipWs();
                if (s.charAt(pos) != ':') throw error("expected ':'");
                pos++;
                skipWs();
                map.put(key, readValue());
                skipWs();
                char c = s.charAt(pos++);
                if (c == '}') return map;
                if (c != ',') throw error("expected ',' or '}'");
            }
        }

        private List<Object> readArray() {
            pos++;
            List<Object> list = new ArrayList<>();
            skipWs();
            if (s.charAt(pos) == ']') {
                pos++;
                return list;
            }
            while (true) {
                skipWs();
                list.add(readValue());
                skipWs();
                char c = s.charAt(pos++);
                if (c == ']') return list;
                if (c != ',') throw error("expected ',' or ']'");
            }
        }

        private String readString() {
            pos++;
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = s.charAt(pos++);
                if (c == '"') return sb.toString();
                if (c == '\\') {
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"', '\\', '/' -> sb.append(e);
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                            pos += 4;
                        }
                        default -> throw error("bad escape");
                    }
                } else if (c < 0x20) {
                    throw error("control character in string");
                } else {
                    sb.append(c);
                }
            }
        }

        private BigDecimal readNumber() {
            int start = pos;
            if (pos < s.length() && s.charAt(pos) == '-') pos++;
            digits();
            if (pos < s.length() && s.charAt(pos) == '.') {
                pos++;
                digits();
            }
            if (pos < s.length() && (s.charAt(pos) == 'e' || s.charAt(pos) == 'E')) {
                pos++;
                if (pos < s.length() && (s.charAt(pos) == '+' || s.charAt(pos) == '-')) pos++;
                digits();
            }
            if (start == pos) throw error("unexpected character '" + s.charAt(pos) + "'");
            try {
                return new BigDecimal(s.substring(start, pos));
            } catch (NumberFormatException e) {
                throw error("bad number");
            }
        }

        private void digits() {
            while (pos < s.length() && Character.isDigit(s.charAt(pos))) pos++;
        }
    }
}
