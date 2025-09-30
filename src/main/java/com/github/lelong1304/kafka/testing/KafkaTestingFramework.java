package com.github.lelong1304.kafka.testing;

import com.github.lelong1304.kafka.testing.config.AvroConfig;
import com.github.lelong1304.kafka.testing.config.DatabaseTestConfig;
import com.github.lelong1304.kafka.testing.config.KafkaTestConfig;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Main entry point for the Kafka Testing Framework with Avro support.
 * This class provides auto-configuration for Kafka, Avro, and database testing components.
 */
@SpringBootApplication
@Import({KafkaTestConfig.class, DatabaseTestConfig.class, AvroConfig.class})
public class KafkaTestingFramework {
    
    public static final String DEFAULT_INPUT_TOPIC = "input-topic";
    public static final String DEFAULT_OUTPUT_TOPIC = "output-topic";
    public static final String DEFAULT_ERROR_TOPIC = "error-topic";
    
}