package io.alerting.core;

/** Which clock stamps {@code created_at}/{@code updated_at} and drives expiry / min-time-to-resolve. */
public enum TimeBasis {
    /** Event time of the first event of the throttle burst (original pipeline behavior). */
    EVENT,
    /** Wall clock when the evaluation runs. */
    PROCESSING
}
