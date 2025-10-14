# Kafka Testing Framework - Usage Guide

This guide demonstrates how to use the schema-agnostic Kafka testing framework for producing and consuming Avro messages using GenericRecord.

## Core Components

### 1. MockSchemaRegistry
In-memory schema registry for testing without external dependencies.

```java
MockSchemaRegistry registry = new MockSchemaRegistry();

// Register a schema
Schema schema = new Schema.Parser().parse(schemaJson);
int schemaId = registry.register("topic-value", schema);

// Retrieve schema by ID
Schema retrieved = registry.getSchemaById(schemaId);

// Get latest schema for a subject
Schema latest = registry.getLatestSchema("topic-value");
```

### 2. GenericAvroSerdes
Factory for creating Avro serializers and deserializers.

```java
MockSchemaRegistry registry = new MockSchemaRegistry();
GenericAvroSerdes serdes = new GenericAvroSerdes(registry);

// Create serializer and deserializer
Serializer<Object> serializer = serdes.createSerializer();
Deserializer<Object> deserializer = serdes.createDeserializer();
```

### 3. GenericAvroProducer
Wrapper around KafkaProducer for producing Avro GenericRecord messages.

```java
try (GenericAvroProducer producer = new GenericAvroProducer(bootstrapServers, serdes)) {
    // Create a GenericRecord
    GenericRecord record = new GenericData.Record(schema);
    record.put("field1", "value1");
    record.put("field2", 42);
    
    // Send message
    producer.send(topic, record).get();
    producer.flush();
}
```

### 4. GenericAvroConsumer
Wrapper around KafkaConsumer for consuming Avro GenericRecord messages.

```java
try (GenericAvroConsumer consumer = new GenericAvroConsumer(bootstrapServers, "group-id", serdes)) {
    consumer.subscribe(topic);
    
    // Poll for messages
    List<ConsumerRecord<String, Object>> records = consumer.poll(Duration.ofSeconds(5));
    
    for (ConsumerRecord<String, Object> record : records) {
        GenericRecord genericRecord = (GenericRecord) record.value();
        // Process the record
    }
}
```

### 5. KafkaTestEnvironment
JUnit 5 extension for Testcontainers Kafka setup (can be used for advanced scenarios).

## Complete Example with Testcontainers

```java
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class MyKafkaTest {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    @Test
    public void testProduceAndConsume() throws Exception {
        // Setup
        MockSchemaRegistry registry = new MockSchemaRegistry();
        GenericAvroSerdes serdes = new GenericAvroSerdes(registry);
        String bootstrapServers = kafka.getBootstrapServers();
        
        // Define schema
        String schemaJson = "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"fields\": ["
            + "{\"name\": \"name\", \"type\": \"string\"},"
            + "{\"name\": \"age\", \"type\": \"int\"}"
            + "]"
            + "}";
        Schema schema = new Schema.Parser().parse(schemaJson);
        
        // Create record
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "Alice");
        record.put("age", 30);
        
        // Produce
        try (GenericAvroProducer producer = new GenericAvroProducer(bootstrapServers, serdes)) {
            producer.send("test-topic", record).get();
            producer.flush();
        }
        
        // Consume
        try (GenericAvroConsumer consumer = new GenericAvroConsumer(bootstrapServers, "test-group", serdes)) {
            consumer.subscribe("test-topic");
            List<ConsumerRecord<String, Object>> messages = consumer.poll(Duration.ofSeconds(5));
            
            GenericRecord received = (GenericRecord) messages.get(0).value();
            assertEquals("Alice", received.get("name").toString());
            assertEquals(30, received.get("age"));
        }
    }
}
```

## Using with Spring Boot (EmbeddedKafka)

The framework is also compatible with Spring Boot's EmbeddedKafka:

```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"test-topic"})
public class SpringKafkaTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    
    @Autowired
    private MockSchemaRegistry mockSchemaRegistry;
    
    @Test
    public void testWithSpring() {
        // Your test code here using the injected components
    }
}
```

## Key Features

1. **No External Dependencies**: Works without a real Schema Registry or Kafka installation
2. **Schema-Agnostic**: Accepts any Avro schema via GenericRecord
3. **Simple API**: Minimal wrapper around standard Kafka clients
4. **Testcontainers Ready**: Easy integration with Testcontainers for true integration tests
5. **Spring Compatible**: Works with both Testcontainers and Spring's EmbeddedKafka

## Serialization Format

The framework uses the following wire format (compatible with Confluent's format):
- Magic Byte (1 byte): 0x0
- Schema ID (4 bytes): Big-endian integer
- Avro Data (variable): Binary-encoded Avro data

This ensures compatibility with standard Avro serialization practices.
