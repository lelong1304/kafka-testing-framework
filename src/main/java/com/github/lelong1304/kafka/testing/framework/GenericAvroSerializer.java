package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Kafka serializer for Avro GenericRecord that uses a mock schema registry.
 * Format: [MAGIC_BYTE(1)][SCHEMA_ID(4)][AVRO_DATA]
 */
public class GenericAvroSerializer implements Serializer<Object> {

    private static final byte MAGIC_BYTE = 0x0;
    private final MockSchemaRegistry schemaRegistry;

    public GenericAvroSerializer(MockSchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed for mock implementation
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }

        if (!(data instanceof GenericRecord)) {
            throw new SerializationException("Data must be a GenericRecord, got: " + data.getClass());
        }

        GenericRecord record = (GenericRecord) data;

        try {
            Schema schema = record.getSchema();
            String subject = topic + (false ? "-key" : "-value"); // Assume value for now
            
            // Register schema and get ID
            int schemaId = schemaRegistry.register(subject, schema);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            
            // Write magic byte
            out.write(MAGIC_BYTE);
            
            // Write schema ID (4 bytes, big-endian)
            out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            
            // Write Avro data
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            writer.write(record, encoder);
            encoder.flush();
            
            return out.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
