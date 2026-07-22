package com.codelry.cdc.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.config.model.MappingConfig;
import com.codelry.cdc.sink.CouchbaseKeyspaceEnsuring.Keyspace;

class CouchbaseKeyspaceEnsuringTest {

    @Test
    void collectsMappingKeyspacesWithDefaults() {
        MappingConfig mapping = new MappingConfig();
        MappingConfig.TableMapping orders = new MappingConfig.TableMapping();
        orders.setSource("public.orders");
        orders.getTarget().setScope("sales");
        orders.getTarget().setCollection("orders");
        MappingConfig.TableMapping items = new MappingConfig.TableMapping();
        items.setSource("public.order_items");
        items.getTarget().setScope("sales");
        // collection omitted → order_items
        mapping.setMappings(List.of(orders, items));

        ConnectionConfig connection = new ConnectionConfig();
        connection.getOffsets().setBackend("file");

        Set<Keyspace> keyspaces = CouchbaseKeyspaceEnsuring.requiredKeyspaces(connection, mapping);
        assertEquals(2, keyspaces.size());
        assertTrue(keyspaces.contains(new Keyspace("sales", "orders")));
        assertTrue(keyspaces.contains(new Keyspace("sales", "order_items")));
    }

    @Test
    void includesOffsetKeyspaceWhenCouchbaseBackendOrHa() {
        MappingConfig mapping = new MappingConfig();
        MappingConfig.TableMapping orders = new MappingConfig.TableMapping();
        orders.setSource("public.orders");
        orders.getTarget().setScope("sales");
        orders.getTarget().setCollection("orders");
        mapping.setMappings(List.of(orders));

        ConnectionConfig connection = new ConnectionConfig();
        connection.getOffsets().setBackend("couchbase");
        connection.getOffsets().getCouchbase().setScope("_default");
        connection.getOffsets().getCouchbase().setCollection("cdc_state");

        Set<Keyspace> keyspaces = CouchbaseKeyspaceEnsuring.requiredKeyspaces(connection, mapping);
        assertTrue(keyspaces.contains(new Keyspace("sales", "orders")));
        assertTrue(keyspaces.contains(new Keyspace("_default", "cdc_state")));
    }

    @Test
    void defaultCollectionNameFromQualifiedTable() {
        assertEquals("orders", CouchbaseKeyspaceEnsuring.defaultCollectionName("public.orders"));
        assertEquals("order_items", CouchbaseKeyspaceEnsuring.defaultCollectionName("ORDER_ITEMS"));
    }
}
