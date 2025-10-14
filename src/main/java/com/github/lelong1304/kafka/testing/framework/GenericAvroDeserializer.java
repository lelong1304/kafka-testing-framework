package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Kafka deserializer for Avro GenericRecord that uses a mock schema registry.
 * Format: [MAGIC_BYTE(1)][SCHEMA_ID(4)][AVRO_DATA]
 */
public class GenericAvroDeserializer implements Deserializer<Object> {

    private static final byte MAGIC_BYTE = 0x0;
    private final MockSchemaRegistry schemaRegistry;

    public GenericAvroDeserializer(MockSchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed for mock implementation
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            
            // Read and verify magic byte
            byte magic = buffer.get();
            if (magic != MAGIC_BYTE) {
                throw new SerializationException("Unknown magic byte: " + magic);
            }
            
            // Read schema ID
            int schemaId = buffer.getInt();
            
            // Get schema from registry
            Schema schema = schemaRegistry.getSchemaById(schemaId);
            if (schema == null) {
                throw new SerializationException("Schema not found for ID: " + schemaId);
            }
            
            // Read Avro data
            int length = buffer.remaining();
            byte[] avroData = new byte[length];
            buffer.get(avroData);
            
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing Avro message", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
