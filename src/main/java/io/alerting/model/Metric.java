package io.alerting.model;

/**
 * What a stream extracts from an event for one rule subject: the observed value and the value it is
 * compared against (relative/ratio thresholds). {@code compare} may be null for absolute-only subjects.
 */
public record Metric(NameValuePair subject, NameValuePair compare) {
    public static Metric of(String subjectName, String subjectValue, String compareName, String compareValue) {
        return new Metric(new NameValuePair(subjectName, subjectValue), new NameValuePair(compareName, compareValue));
    }

    public static Metric of(String subjectName, String subjectValue) {
        return new Metric(new NameValuePair(subjectName, subjectValue), null);
    }
}
