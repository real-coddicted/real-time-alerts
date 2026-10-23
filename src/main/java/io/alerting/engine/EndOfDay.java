package io.alerting.engine;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/** Business-date helpers. Dates are {@code yyyymmdd} ints. */
public final class EndOfDay {
    private EndOfDay() {}

    public static LocalDate toLocalDate(int yyyymmdd) {
        return LocalDate.of(yyyymmdd / 10000, (yyyymmdd / 100) % 100, yyyymmdd % 100);
    }

    public static int toInt(LocalDate d) {
        return d.getYear() * 10000 + d.getMonthValue() * 100 + d.getDayOfMonth();
    }

    public static int dateOf(long epochMillis, ZoneId zone) {
        return toInt(Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalDate());
    }

    /** Millis until the last millisecond of {@code date} in {@code zone}, or of today if {@code date} is past. */
    public static long millisUntilEndOfDay(int date, long nowMillis, ZoneId zone) {
        LocalDate d = toLocalDate(date);
        LocalDate today = Instant.ofEpochMilli(nowMillis).atZone(zone).toLocalDate();
        LocalDate base = d.isAfter(today) ? d : today;
        long endOfDay = base.plusDays(1).atStartOfDay(zone).toInstant().toEpochMilli() - 1;
        return Math.max(0, endOfDay - nowMillis);
    }
}
