package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Thin wrapper around KafkaProducer for producing Avro GenericRecord messages.
 */
public class GenericAvroProducer implements AutoCloseable {

    private final KafkaProducer<String, Object> producer;

    /**
     * Create a new GenericAvroProducer.
     * 
     * @param bootstrapServers Kafka bootstrap servers
     * @param serdes the GenericAvroSerdes factory
     */
    public GenericAvroProducer(String bootstrapServers, GenericAvroSerdes serdes) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        this.producer = new KafkaProducer<>(props, new StringSerializer(), serdes.createSerializer());
    }

    /**
     * Send a message to a topic.
     * 
     * @param topic the topic to send to
     * @param record the GenericRecord to send
     * @return a Future for the RecordMetadata
     */
    public Future<RecordMetadata> send(String topic, GenericRecord record) {
        return producer.send(new ProducerRecord<>(topic, record));
    }

    /**
     * Send a message with a key to a topic.
     * 
     * @param topic the topic to send to
     * @param key the message key
     * @param record the GenericRecord to send
     * @return a Future for the RecordMetadata
     */
    public Future<RecordMetadata> send(String topic, String key, GenericRecord record) {
        return producer.send(new ProducerRecord<>(topic, key, record));
    }

    /**
     * Flush any pending messages.
     */
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}
