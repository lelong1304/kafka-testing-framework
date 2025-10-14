package com.github.lelong1304.kafka.testing.config;

import com.github.lelong1304.kafka.testing.framework.GenericAvroDeserializer;
import com.github.lelong1304.kafka.testing.framework.GenericAvroSerializer;
import com.github.lelong1304.kafka.testing.framework.MockSchemaRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

@TestConfiguration
public class AvroConfig {

    private static final String SCHEMA_REGISTRY_URL = "mock://test-registry";

    @Bean
    public MockSchemaRegistry mockSchemaRegistry() {
        return new MockSchemaRegistry();
    }

    @Bean
    @Primary
    public ProducerFactory<String, Object> avroProducerFactory(EmbeddedKafkaBroker embeddedKafka, MockSchemaRegistry mockSchemaRegistry) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(producerProps);
        factory.setValueSerializer(new GenericAvroSerializer(mockSchemaRegistry));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> avroKafkaTemplate(ProducerFactory<String, Object> avroProducerFactory) {
        return new KafkaTemplate<>(avroProducerFactory);
    }

    @Bean
    @Primary
    public ConsumerFactory<String, Object> avroConsumerFactory(EmbeddedKafkaBroker embeddedKafka, MockSchemaRegistry mockSchemaRegistry) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("avro-test-group", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        DefaultKafkaConsumerFactory<String, Object> factory = new DefaultKafkaConsumerFactory<>(consumerProps);
        factory.setValueDeserializer(new GenericAvroDeserializer(mockSchemaRegistry));
        return factory;
    }

    @Bean
    public String schemaRegistryUrl() {
        return SCHEMA_REGISTRY_URL;
    }
}