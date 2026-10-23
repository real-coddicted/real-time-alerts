package io.alerting.model;

import java.util.Objects;

/**
 * What a stream extracts from an event for one rule subject: the observed value and the value it is
 * compared against (relative/ratio thresholds). {@code compare} may be null for absolute-only subjects.
 */
public final class Metric {
    private final NameValuePair subject;
    private final NameValuePair compare;

    public Metric(NameValuePair subject, NameValuePair compare) {
        this.subject = Objects.requireNonNull(subject, "subject");
        this.compare = compare;
    }

    public static Metric of(String subjectName, String subjectValue, String compareName, String compareValue) {
        return new Metric(new NameValuePair(subjectName, subjectValue), new NameValuePair(compareName, compareValue));
    }

    public static Metric of(String subjectName, String subjectValue) {
        return new Metric(new NameValuePair(subjectName, subjectValue), null);
    }

    public NameValuePair subject() {
        return subject;
    }

    public NameValuePair compare() {
        return compare;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Metric)) return false;
        Metric that = (Metric) o;
        return subject.equals(that.subject) && Objects.equals(compare, that.compare);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, compare);
    }

    @Override
    public String toString() {
        return "Metric{" + subject + ", " + compare + "}";
    }
}
