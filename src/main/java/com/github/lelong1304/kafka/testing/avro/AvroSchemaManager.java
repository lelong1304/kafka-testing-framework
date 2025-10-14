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

        String schemaString = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
        Schema schema = new Schema.Parser().parse(schemaString);
        schemaCache.put(subject, schema);
        return schema;
    }

    public Schema getSchemaById(int id) throws Exception {
        Object rawSchema = schemaRegistryClient.getSchemaById(id).rawSchema();
        if (rawSchema instanceof Schema) {
            return (Schema) rawSchema;
        }
        return new Schema.Parser().parse(rawSchema.toString());
    }

    public void clearCache() {
        schemaCache.clear();
    }
}