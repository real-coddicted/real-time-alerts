package io.alerting.rules;

import io.alerting.model.EntityKey;
import io.alerting.model.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable snapshot of active rules, indexed by (subject, scope). This is the rules side input value:
 * broadcast to every worker and replaced wholesale on each successful refresh.
 */
public final class RuleCatalog implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RuleCatalog.class);

    private final long version;
    private final Map<String, Rule> byId;
    private final Map<String, List<Rule>> bySubjectScope;

    private RuleCatalog(long version, Map<String, Rule> byId) {
        this.version = version;
        this.byId = Collections.unmodifiableMap(byId);
        Map<String, List<Rule>> index = new HashMap<>();
        byId.values().stream()
                .sorted(Comparator.comparing(Rule::id))
                .forEach(r -> index.computeIfAbsent(indexKey(r.subject(), r.scope()), k -> new ArrayList<>()).add(r));
        this.bySubjectScope = index;
    }

    /** Inactive rules are dropped; for duplicate ids the highest version wins. */
    public static RuleCatalog of(long version, Collection<Rule> rules) {
        Map<String, Rule> byId = new LinkedHashMap<>();
        for (Rule r : rules) {
            if (!r.active()) continue;
            Rule existing = byId.get(r.id());
            if (existing != null) {
                LOG.warn("duplicate rule id {}; keeping highest version", r.id());
                if (existing.version() >= r.version()) continue;
            }
            byId.put(r.id(), r);
        }
        return new RuleCatalog(version, byId);
    }

    private static String indexKey(String subject, String scope) {
        return subject + '\u0000' + scope;
    }

    public long version() {
        return version;
    }

    public int size() {
        return byId.size();
    }

    public Rule get(String id) {
        return byId.get(id);
    }

    public Collection<Rule> rules() {
        return byId.values();
    }

    /** Rules for any of {@code subjects} that match {@code key}, ordered by id. */
    public List<Rule> match(EntityKey key, Collection<String> subjects) {
        List<Rule> out = new ArrayList<>();
        for (String subject : subjects) {
            for (Rule r : bySubjectScope.getOrDefault(indexKey(subject, key.scope()), List.of())) {
                if (RuleMatcher.matches(r, key)) out.add(r);
            }
        }
        out.sort(Comparator.comparing(Rule::id));
        return out;
    }

    @Override
    public String toString() {
        return "RuleCatalog{v" + version + ", " + byId.size() + " rules}";
    }
}
