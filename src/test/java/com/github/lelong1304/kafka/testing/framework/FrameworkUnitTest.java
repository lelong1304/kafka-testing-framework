package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Kafka testing framework core classes.
 * These tests verify that the framework works without requiring Kafka or Testcontainers.
 */
public class FrameworkUnitTest {

    @Test
    public void testMockSchemaRegistry() {
        MockSchemaRegistry registry = new MockSchemaRegistry();
        
        // Create a simple schema
        String schemaJson = "{" +
            "\"type\": \"record\"," +
            "\"name\": \"User\"," +
            "\"namespace\": \"com.example\"," +
            "\"fields\": [" +
            "{\"name\": \"name\", \"type\": \"string\"}," +
            "{\"name\": \"age\", \"type\": \"int\"}" +
            "]" +
            "}";
        Schema schema = new Schema.Parser().parse(schemaJson);
        
        // Register schema
        int id1 = registry.register("test-topic-value", schema);
        assertTrue(id1 > 0, "Schema ID should be positive");
        
        // Register same schema again - should return same ID
        int id2 = registry.register("test-topic-value", schema);
        assertEquals(id1, id2, "Same schema should return same ID");
        
        // Retrieve schema by ID
        Schema retrievedSchema = registry.getSchemaById(id1);
        assertNotNull(retrievedSchema, "Schema should be retrievable by ID");
        assertEquals(schema, retrievedSchema, "Retrieved schema should match original");
        
        // Get latest schema for subject
        Schema latestSchema = registry.getLatestSchema("test-topic-value");
        assertNotNull(latestSchema, "Latest schema should be retrievable");
        assertEquals(schema, latestSchema, "Latest schema should match registered schema");
    }

    @Test
    public void testSerializationRoundTrip() {
        MockSchemaRegistry registry = new MockSchemaRegistry();
        GenericAvroSerializer serializer = new GenericAvroSerializer(registry);
        GenericAvroDeserializer deserializer = new GenericAvroDeserializer(registry);
        
        // Create a simple schema and record
        String schemaJson = "{" +
            "\"type\": \"record\"," +
            "\"name\": \"Person\"," +
            "\"namespace\": \"com.example\"," +
            "\"fields\": [" +
            "{\"name\": \"name\", \"type\": \"string\"}," +
            "{\"name\": \"age\", \"type\": \"int\"}," +
            "{\"name\": \"email\", \"type\": \"string\"}" +
            "]" +
            "}";
        Schema schema = new Schema.Parser().parse(schemaJson);
        
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "John Doe");
        record.put("age", 30);
        record.put("email", "john@example.com");
        
        // Serialize
        byte[] serialized = serializer.serialize("test-topic", record);
        assertNotNull(serialized, "Serialized data should not be null");
        assertTrue(serialized.length > 0, "Serialized data should have content");
        
        // Deserialize
        Object deserialized = deserializer.deserialize("test-topic", serialized);
        assertNotNull(deserialized, "Deserialized data should not be null");
        assertTrue(deserialized instanceof GenericRecord, "Deserialized data should be GenericRecord");
        
        GenericRecord deserializedRecord = (GenericRecord) deserialized;
        assertEquals("John Doe", deserializedRecord.get("name").toString(), "Name should match");
        assertEquals(30, deserializedRecord.get("age"), "Age should match");
        assertEquals("john@example.com", deserializedRecord.get("email").toString(), "Email should match");
    }

    @Test
    public void testSerializationWithNullValue() {
        MockSchemaRegistry registry = new MockSchemaRegistry();
        GenericAvroSerializer serializer = new GenericAvroSerializer(registry);
        GenericAvroDeserializer deserializer = new GenericAvroDeserializer(registry);
        
        // Serialize null
        byte[] serialized = serializer.serialize("test-topic", null);
        assertNull(serialized, "Serialized null should be null");
        
        // Deserialize null
        Object deserialized = deserializer.deserialize("test-topic", null);
        assertNull(deserialized, "Deserialized null should be null");
    }

    @Test
    public void testGenericAvroSerdes() {
        MockSchemaRegistry registry = new MockSchemaRegistry();
        GenericAvroSerdes serdes = new GenericAvroSerdes(registry);
        
        assertNotNull(serdes.createSerializer(), "Serializer should not be null");
        assertNotNull(serdes.createDeserializer(), "Deserializer should not be null");
        assertSame(registry, serdes.getSchemaRegistry(), "Schema registry should be same instance");
    }

    @Test
    public void testSchemaRegistryClear() {
        MockSchemaRegistry registry = new MockSchemaRegistry();
        
        // Create and register a schema
        String schemaJson = "{" +
            "\"type\": \"record\"," +
            "\"name\": \"Test\"," +
            "\"fields\": [{\"name\": \"field1\", \"type\": \"string\"}]" +
            "}";
        Schema schema = new Schema.Parser().parse(schemaJson);
        int id = registry.register("test-subject", schema);
        
        // Verify it's registered
        assertNotNull(registry.getSchemaById(id), "Schema should be registered");
        assertNotNull(registry.getLatestSchema("test-subject"), "Latest schema should be available");
        
        // Clear registry
        registry.clear();
        
        // Verify it's cleared
        assertNull(registry.getSchemaById(id), "Schema should be cleared");
        assertNull(registry.getLatestSchema("test-subject"), "Latest schema should be cleared");
    }
}
