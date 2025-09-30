package com.github.lelong1304.kafka.testing.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class AvroSchemaManager {

    private final SchemaRegistryClient schemaRegistryClient;
    private final ConcurrentMap<String, Schema> schemaCache = new ConcurrentHashMap<>();

    public AvroSchemaManager() {
        this.schemaRegistryClient = new MockSchemaRegistryClient();
    }

    public void registerSchema(String subject, Schema schema) throws Exception {
        schemaRegistryClient.register(subject, schema);
        schemaCache.put(subject, schema);
    }

    public Schema getLatestSchema(String subject) throws Exception {
        Schema cached = schemaCache.get(subject);
        if (cached != null) {
            return cached;
        }
        
        Schema schema = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
        schemaCache.put(subject, schema);
        return schema;
    }

    public Schema getSchemaById(int id) throws Exception {
        return schemaRegistryClient.getSchemaById(id);
    }

    public void clearCache() {
        schemaCache.clear();
    }
}