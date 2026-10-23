package io.alerting.testing;

import java.util.Objects;

public final class Assert {
    private Assert() {}

    public static void assertEquals(Object expected, Object actual) {
        assertEquals(expected, actual, null);
    }

    public static void assertEquals(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError((message == null ? "" : message + ": ") + "expected <" + expected + "> but was <" + actual + ">");
        }
    }

    public static void assertNotEquals(Object a, Object b) {
        if (Objects.equals(a, b)) throw new AssertionError("expected values to differ: <" + a + ">");
    }

    public static void assertTrue(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void assertFalse(boolean condition, String message) {
        if (condition) throw new AssertionError(message);
    }

    public static void assertNull(Object o, String message) {
        if (o != null) throw new AssertionError(message + ": expected null but was <" + o + ">");
    }

    public static <T extends Throwable> T assertThrows(Class<T> type, Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            if (type.isInstance(t)) return type.cast(t);
            throw new AssertionError("expected " + type.getSimpleName() + " but got " + t, t);
        }
        throw new AssertionError("expected " + type.getSimpleName() + " to be thrown");
    }
}
