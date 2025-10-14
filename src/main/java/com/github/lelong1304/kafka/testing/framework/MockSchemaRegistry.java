package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.Schema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock implementation of a schema registry for testing.
 * Stores schemas in memory without requiring an external Schema Registry service.
 */
public class MockSchemaRegistry {
    
    private final Map<String, Map<Schema, Integer>> subjectToSchemaIds = new ConcurrentHashMap<>();
    private final Map<Integer, Schema> idToSchema = new ConcurrentHashMap<>();
    private final Map<String, Schema> subjectToLatestSchema = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(1);

    /**
     * Register a schema for a subject.
     * 
     * @param subject the subject name (typically topic-key or topic-value)
     * @param schema the Avro schema to register
     * @return the schema ID
     */
    public synchronized int register(String subject, Schema schema) {
        Map<Schema, Integer> schemaIds = subjectToSchemaIds.computeIfAbsent(subject, k -> new ConcurrentHashMap<>());
        
        // Check if schema already exists for this subject
        Integer existingId = schemaIds.get(schema);
        if (existingId != null) {
            return existingId;
        }
        
        // Register new schema
        int id = nextId.getAndIncrement();
        schemaIds.put(schema, id);
        idToSchema.put(id, schema);
        subjectToLatestSchema.put(subject, schema);
        
        return id;
    }

    /**
     * Get a schema by its ID.
     * 
     * @param id the schema ID
     * @return the schema, or null if not found
     */
    public Schema getSchemaById(int id) {
        return idToSchema.get(id);
    }

    /**
     * Get the latest schema for a subject.
     * 
     * @param subject the subject name
     * @return the latest schema, or null if not found
     */
    public Schema getLatestSchema(String subject) {
        return subjectToLatestSchema.get(subject);
    }

    /**
     * Get the schema ID for a given subject and schema.
     * 
     * @param subject the subject name
     * @param schema the schema
     * @return the schema ID, or null if not found
     */
    public Integer getSchemaId(String subject, Schema schema) {
        Map<Schema, Integer> schemaIds = subjectToSchemaIds.get(subject);
        return schemaIds != null ? schemaIds.get(schema) : null;
    }

    /**
     * Clear all registered schemas.
     */
    public void clear() {
        subjectToSchemaIds.clear();
        idToSchema.clear();
        subjectToLatestSchema.clear();
        nextId.set(1);
    }
}
