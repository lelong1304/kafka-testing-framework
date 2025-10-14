package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Wrapper around KafkaConsumer for consuming Avro GenericRecord messages.
 */
public class GenericAvroConsumer implements AutoCloseable {

    private final KafkaConsumer<String, Object> consumer;

    /**
     * Create a new GenericAvroConsumer.
     * 
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId the consumer group ID
     * @param serdes the GenericAvroSerdes factory
     */
    public GenericAvroConsumer(String bootstrapServers, String groupId, GenericAvroSerdes serdes) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), serdes.createDeserializer());
    }

    /**
     * Subscribe to a topic.
     * 
     * @param topic the topic to subscribe to
     */
    public void subscribe(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * Poll for messages.
     * 
     * @param timeout the timeout duration
     * @return list of consumer records
     */
    public List<ConsumerRecord<String, Object>> poll(Duration timeout) {
        ConsumerRecords<String, Object> records = consumer.poll(timeout);
        List<ConsumerRecord<String, Object>> result = new ArrayList<>();
        records.forEach(result::add);
        return result;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
