package com.codelry.cdc.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.codelry.cdc.config.ConfigValidationException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;

class CouchbaseSinkDurabilityTest {

    @Test
    void parsesDurabilityLevels() {
        assertEquals(DurabilityLevel.NONE, CouchbaseSink.parseDurability("none"));
        assertEquals(DurabilityLevel.MAJORITY, CouchbaseSink.parseDurability("majority"));
        assertEquals(DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                CouchbaseSink.parseDurability("majorityAndPersistActive"));
        assertEquals(DurabilityLevel.PERSIST_TO_MAJORITY, CouchbaseSink.parseDurability("persistToMajority"));
        assertEquals(DurabilityLevel.NONE, CouchbaseSink.parseDurability(null));
    }

    @Test
    void rejectsInvalidDurability() {
        assertThrows(ConfigValidationException.class, () -> CouchbaseSink.parseDurability("superStrong"));
    }
}
