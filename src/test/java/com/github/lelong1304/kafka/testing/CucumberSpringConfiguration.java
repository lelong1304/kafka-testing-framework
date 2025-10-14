package com.github.lelong1304.kafka.testing;

import com.github.lelong1304.kafka.testing.config.AvroConfig;
import com.github.lelong1304.kafka.testing.config.DatabaseTestConfig;
import com.github.lelong1304.kafka.testing.config.KafkaTestConfig;
import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Spring Boot test configuration for Cucumber tests.
 * Configures embedded Kafka and H2 database for testing.
 */
@CucumberContextConfiguration
@SpringBootTest(classes = KafkaTestingFramework.class)
@EmbeddedKafka(
    partitions = 6,
    topics = {"orders", "processed-orders", "error-orders", "input-topic", "output-topic"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
    }
)
@Import({KafkaTestConfig.class, AvroConfig.class, DatabaseTestConfig.class})
public class CucumberSpringConfiguration {
}
