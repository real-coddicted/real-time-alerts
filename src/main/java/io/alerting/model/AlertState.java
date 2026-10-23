package io.alerting.model;

public enum AlertState {
    /** First breach; a new alert id is minted. */
    FIRED,
    /** Still active and the value changed (or keep-ticking is on). */
    FIRED_UPDATED,
    /** The resolve threshold was met. */
    RESOLVED_AUTOMATICALLY,
    /** Rule expiry elapsed while not breaching, or end of business day. */
    RESOLVED_EXPIRED,
    /** The rule was deleted, deactivated or no longer matches the key. Not in the original pipeline. */
    RESOLVED_RULE_REMOVED;

    public boolean isResolved() {
        return this != FIRED && this != FIRED_UPDATED;
    }
}
