package io.alerting.engine;

import io.alerting.model.EntityKey;

/** Alert state is partitioned by stream and key: the same key on two streams is independent. */
record StateKey(String stream, EntityKey key) {}
