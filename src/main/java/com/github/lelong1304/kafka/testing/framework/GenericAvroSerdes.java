package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Factory for creating Avro serializers and deserializers that work with GenericRecord
 * and a mock schema registry.
 */
public class GenericAvroSerdes {

    private final MockSchemaRegistry schemaRegistry;

    public GenericAvroSerdes(MockSchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    /**
     * Create a serializer for Object (GenericRecord).
     * 
     * @return an Object serializer
     */
    public Serializer<Object> createSerializer() {
        return new GenericAvroSerializer(schemaRegistry);
    }

    /**
     * Create a deserializer for Object (GenericRecord).
     * 
     * @return an Object deserializer
     */
    public Deserializer<Object> createDeserializer() {
        return new GenericAvroDeserializer(schemaRegistry);
    }

    /**
     * Get the schema registry used by this factory.
     * 
     * @return the mock schema registry
     */
    public MockSchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }
}
