package com.github.lelong1304.kafka.testing.framework;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test demonstrating the use of the Kafka testing framework
 * with Testcontainers to produce and consume Avro messages.
 */
@Testcontainers
public class KafkaAvroIntegrationTest {

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    private static MockSchemaRegistry schemaRegistry;
    private static GenericAvroSerdes serdes;
    private static String bootstrapServers;

    @BeforeAll
    public static void setup() {
        bootstrapServers = kafkaContainer.getBootstrapServers();
        schemaRegistry = new MockSchemaRegistry();
        serdes = new GenericAvroSerdes(schemaRegistry);
    }

    @AfterAll
    public static void tearDown() {
        if (schemaRegistry != null) {
            schemaRegistry.clear();
        }
    }

    @Test
    public void testProduceAndConsumeAvroMessage() throws Exception {
        // Define a simple Avro schema
        String schemaJson = "{" +
            "\"type\": \"record\"," +
            "\"name\": \"TestMessage\"," +
            "\"namespace\": \"com.example.test\"," +
            "\"fields\": [" +
            "{\"name\": \"id\", \"type\": \"string\"}," +
            "{\"name\": \"value\", \"type\": \"int\"}," +
            "{\"name\": \"message\", \"type\": \"string\"}" +
            "]" +
            "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Create a GenericRecord
        GenericRecord testRecord = new GenericData.Record(schema);
        testRecord.put("id", "test-123");
        testRecord.put("value", 42);
        testRecord.put("message", "Hello Kafka with Avro!");

        String topic = "test-topic";

        // Create producer and send message
        try (GenericAvroProducer producer = new GenericAvroProducer(bootstrapServers, serdes)) {
            producer.send(topic, testRecord).get(10, TimeUnit.SECONDS);
            producer.flush();
        }

        // Create consumer and receive message
        try (GenericAvroConsumer consumer = new GenericAvroConsumer(bootstrapServers, "test-group", serdes)) {
            consumer.subscribe(topic);
            
            // Poll for messages with timeout
            List<ConsumerRecord<String, Object>> messages = null;
            for (int i = 0; i < 10; i++) {
                messages = consumer.poll(Duration.ofSeconds(1));
                if (!messages.isEmpty()) {
                    break;
                }
            }

            // Verify message was received
            assertNotNull(messages, "Messages should not be null");
            assertFalse(messages.isEmpty(), "Should receive at least one message");

            // Verify message content
            ConsumerRecord<String, Object> record = messages.get(0);
            assertNotNull(record.value(), "Record value should not be null");
            assertTrue(record.value() instanceof GenericRecord, "Value should be a GenericRecord");

            GenericRecord receivedRecord = (GenericRecord) record.value();
            assertEquals("test-123", receivedRecord.get("id").toString(), "ID should match");
            assertEquals(42, receivedRecord.get("value"), "Value should match");
            assertEquals("Hello Kafka with Avro!", receivedRecord.get("message").toString(), "Message should match");
        }
    }

    @Test
    public void testProduceAndConsumeMultipleMessages() throws Exception {
        String schemaJson = "{" +
            "\"type\": \"record\"," +
            "\"name\": \"Counter\"," +
            "\"namespace\": \"com.example.test\"," +
            "\"fields\": [" +
            "{\"name\": \"count\", \"type\": \"int\"}" +
            "]" +
            "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        String topic = "multi-message-topic";

        // Produce multiple messages
        try (GenericAvroProducer producer = new GenericAvroProducer(bootstrapServers, serdes)) {
            for (int i = 1; i <= 5; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("count", i);
                producer.send(topic, record).get(10, TimeUnit.SECONDS);
            }
            producer.flush();
        }

        // Consume and verify all messages
        try (GenericAvroConsumer consumer = new GenericAvroConsumer(bootstrapServers, "multi-group", serdes)) {
            consumer.subscribe(topic);
            
            // Collect all messages
            List<ConsumerRecord<String, Object>> allMessages = new java.util.ArrayList<>();
            for (int i = 0; i < 20; i++) {
                List<ConsumerRecord<String, Object>> batch = consumer.poll(Duration.ofSeconds(1));
                allMessages.addAll(batch);
                if (allMessages.size() >= 5) {
                    break;
                }
            }

            // Verify we received all 5 messages
            assertEquals(5, allMessages.size(), "Should receive 5 messages");

            // Verify message contents
            for (int i = 0; i < 5; i++) {
                GenericRecord record = (GenericRecord) allMessages.get(i).value();
                assertEquals(i + 1, record.get("count"), "Count should match");
            }
        }
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        // Original schema
        String schemaV1Json = "{" +
            "\"type\": \"record\"," +
            "\"name\": \"User\"," +
            "\"namespace\": \"com.example.test\"," +
            "\"fields\": [" +
            "{\"name\": \"name\", \"type\": \"string\"}," +
            "{\"name\": \"age\", \"type\": \"int\"}" +
            "]" +
            "}";
        Schema schemaV1 = new Schema.Parser().parse(schemaV1Json);

        // Create and send message with v1 schema
        GenericRecord recordV1 = new GenericData.Record(schemaV1);
        recordV1.put("name", "Alice");
        recordV1.put("age", 30);

        String topic = "evolution-topic";

        try (GenericAvroProducer producer = new GenericAvroProducer(bootstrapServers, serdes)) {
            producer.send(topic, recordV1).get(10, TimeUnit.SECONDS);
            producer.flush();
        }

        // Consume with the same schema
        try (GenericAvroConsumer consumer = new GenericAvroConsumer(bootstrapServers, "evolution-group", serdes)) {
            consumer.subscribe(topic);
            
            List<ConsumerRecord<String, Object>> messages = null;
            for (int i = 0; i < 10; i++) {
                messages = consumer.poll(Duration.ofSeconds(1));
                if (!messages.isEmpty()) {
                    break;
                }
            }

            assertNotNull(messages, "Messages should not be null");
            assertFalse(messages.isEmpty(), "Should receive at least one message");

            GenericRecord receivedRecord = (GenericRecord) messages.get(0).value();
            assertEquals("Alice", receivedRecord.get("name").toString(), "Name should match");
            assertEquals(30, receivedRecord.get("age"), "Age should match");
        }
    }
}
