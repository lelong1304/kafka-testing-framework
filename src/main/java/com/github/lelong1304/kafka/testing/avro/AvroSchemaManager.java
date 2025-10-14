package com.github.lelong1304.kafka.testing.avro;

import com.github.lelong1304.kafka.testing.framework.MockSchemaRegistry;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class AvroSchemaManager {

    private final MockSchemaRegistry schemaRegistry;
    private final ConcurrentMap<String, Schema> schemaCache = new ConcurrentHashMap<>();

    public AvroSchemaManager() {
        this.schemaRegistry = new MockSchemaRegistry();
    }

    public void registerSchema(String subject, Schema schema) throws Exception {
        schemaRegistry.register(subject, schema);
        schemaCache.put(subject, schema);
    }
    
    public Schema getLatestSchema(String subject) throws Exception {
        Schema cached = schemaCache.get(subject);
        if (cached != null) {
            return cached;
        }

        Schema schema = schemaRegistry.getLatestSchema(subject);
        if (schema != null) {
            schemaCache.put(subject, schema);
        }
        return schema;
    }

    public Schema getSchemaById(int id) throws Exception {
        return schemaRegistry.getSchemaById(id);
    }

    public void clearCache() {
        schemaCache.clear();
    }
    
    public MockSchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }
}
