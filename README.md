# Kafka Testing Framework with Avro Support

A comprehensive Java library that enables behavior-driven testing of Kafka applications using Gherkin syntax with **Avro message format support** and database verification capabilities.

[![Java](https://img.shields.io/badge/Java-11+-orange.svg)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-2.7+-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.0+-red.svg)](https://kafka.apache.org/)
[![Avro](https://img.shields.io/badge/Apache%20Avro-1.11+-blue.svg)](https://avro.apache.org/)

## 🚀 Features

- 🥒 **Gherkin-based Test Scenarios**: Write tests in natural language using Cucumber/Gherkin
- 📨 **Kafka Integration**: Easy setup for embedded Kafka clusters and message verification
- 🔄 **Avro Message Support**: Full support for Avro serialization with schema registry
- 🗄️ **Database State Verification**: Support for multiple database types with before/after state comparison
- ⚡ **Asynchronous Testing**: Handle async Kafka message processing with configurable timeouts
- 🧪 **Test Isolation**: Each test scenario starts with a clean state
- 📋 **Schema Evolution**: Test schema compatibility and evolution
- 🔧 **Production Ready**: Comprehensive error handling and logging

## 📦 Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>com.github.lelong1304</groupId>
    <artifactId>kafka-testing-framework</artifactId>
    <version>1.0.0</version>
    <scope>test</scope>
</dependency>
```

### 2. Create Test Configuration

```java
@SpringBootTest(classes = KafkaTestingFramework.class)
@EmbeddedKafka(partitions = 1, topics = {"orders", "processed-orders"})
@CucumberContextConfiguration
public class KafkaAvroTestConfiguration {
}
```

### 3. Write Gherkin Features with Avro

```gherkin
Feature: Order Processing with Avro

  Background:
    Given the following Avro schema is registered for topic "orders":
      """
      {
        "type": "record",
        "name": "Order",
        "fields": [
          {"name": "orderId", "type": "string"},
          {"name": "customerId", "type": "long"},
          {"name": "amount", "type": "double"}
        ]
      }
      """

  Scenario: Process valid Avro order
    Given the database has the following customers:
      | id | name     | email            |
      | 1  | John Doe | john@example.com |
    When I send an Avro message to topic "orders":
      | field      | value   |
      | orderId    | 12345   |
      | customerId | 1       |
      | amount     | 100.50  |
    Then I should receive an Avro message on topic "processed-orders" within 5 seconds
    And the Avro message should have:
      | field      | value     |
      | orderId    | 12345     |
      | status     | PROCESSED |
    And the database should have the following orders:
      | order_id | customer_id | status    |
      | 12345    | 1          | PROCESSED |
```

## 📖 Available Step Definitions

### Avro Schema Management

```gherkin
# Register an Avro schema for a topic
Given the following Avro schema is registered for topic "topic-name":
  """
  {
    "type": "record",
    "name": "MyRecord",
    "fields": [...]
  }
  """
```

### Avro Message Production

```gherkin
# Send an Avro message to a topic
When I send an Avro message to topic "orders":
  | field      | value   |
  | orderId    | 12345   |
  | customerId | 1       |
  | amount     | 100.50  |

# Send an Avro message with a specific key
When I send an Avro message with key "customer-1" to topic "orders":
  | field      | value   |
  | orderId    | 12345   |
  | customerId | 1       |
```

### Avro Message Consumption & Verification

```gherkin
# Verify Avro message reception
Then I should receive an Avro message on topic "processed-orders" within 10 seconds
Then I should receive exactly 2 Avro messages on topic "orders" within 15 seconds

# Verify Avro message content
And the Avro message should have:
  | field      | value     |
  | orderId    | 12345     |
  | status     | PROCESSED |

# Verify specific field
And the Avro message field "orderId" should be "12345"
```

### Kafka Steps (JSON/String Messages)

```gherkin
# Topic state management
And the input topic "topic-name" is empty
And the output topic "topic-name" is empty

# Message production
When I send a message to topic "orders":
  """
  {"orderId": "12345", "amount": 100.50}
  """

# Message verification
Then I should receive a message on topic "processed-orders" within 5 seconds
And the message should contain:
  """
  {"status": "PROCESSED"}
  """
```

### Database Steps

```gherkin
# Data setup
Given the database has the following customers:
  | id | name     | email            |
  | 1  | John Doe | john@example.com |

Given the database table "products" has the following data:
  | id | name   | price |
  | 1  | Laptop | 999.99|

# Data verification
And the database should have the following orders:
  | order_id | status    |
  | 12345    | PROCESSED |

And the database table "orders" should contain:
  | order_id | customer_id |
  | 12345    | 1          |

And the database table "audit_log" should be empty
And the database table "orders" should have 3 rows
```

## ⚙️ Configuration

### Avro Configuration

The framework automatically configures:
- **Mock Schema Registry** for testing
- **Avro Serializers/Deserializers** for Kafka
- **Schema Evolution** support

### Database Configuration

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1
    driver-class-name: org.h2.Driver
  sql:
    init:
      schema-locations: classpath:schema.sql
```

### Kafka Configuration

```java
@EmbeddedKafka(
    partitions = 1,
    topics = {"orders", "processed-orders", "error-orders"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
    }
)
```

## 🔧 Advanced Usage

### Custom Avro Schema Management

```java
@Component
public class CustomSchemaManager {
    
    @Autowired
    private AvroSchemaManager schemaManager;
    
    @PostConstruct
    public void registerSchemas() throws Exception {
        Schema orderSchema = loadSchemaFromFile("Order.avsc");
        schemaManager.registerSchema("orders-value", orderSchema);
    }
}
```

### Complex Avro Types

The framework supports:
- **Union Types**: `["null", "string"]` for nullable fields
- **Array Types**: `{"type": "array", "items": "string"}`
- **Nested Records**: Complex object hierarchies
- **Logical Types**: Decimal, timestamps, etc.

Example with complex types:

```gherkin
When I send an Avro message to topic "orders":
  | field        | value                           |
  | orderId      | ORDER-123                      |
  | items        | ["laptop", "mouse", "keyboard"] |
  | metadata     | {"source": "web", "priority": 1}|
  | createdAt    | 1640995200000                  |
```

### Schema Evolution Testing

```gherkin
Scenario: Test backward compatibility
  Given the following Avro schema is registered for topic "orders":
    """
    {
      "type": "record",
      "name": "Order",
      "fields": [
        {"name": "orderId", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "newField", "type": ["null", "string"], "default": null}
      ]
    }
    """
  When I send an Avro message to topic "orders":
    | field    | value   |
    | orderId  | 12345   |
    | amount   | 100.50  |
    | newField | premium |
  Then the message should be processed without errors
```

### Custom Database Schema

Create `src/test/resources/schema.sql`:

```sql
CREATE TABLE customers (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);
```

## 📝 Example Projects

### E-commerce Order Processing

```gherkin
Feature: E-commerce Order Processing

  Background:
    Given the following Avro schema is registered for topic "orders":
      """
      {
        "type": "record",
        "name": "Order",
        "namespace": "com.ecommerce",
        "fields": [
          {"name": "orderId", "type": "string"},
          {"name": "customerId", "type": "long"},
          {"name": "items", "type": {
            "type": "array", 
            "items": {
              "type": "record",
              "name": "OrderItem",
              "fields": [
                {"name": "productId", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "price", "type": "double"}
              ]
            }
          }},
          {"name": "totalAmount", "type": "double"},
          {"name": "timestamp", "type": "long"}
        ]
      }
      """

  Scenario: Process complex order with multiple items
    Given the database has the following customers:
      | id | name        | email               |
      | 1  | Alice Smith | alice@example.com   |
    And the database has the following products:
      | id | name   | price | stock |
      | P1 | Laptop | 999.99| 10    |
      | P2 | Mouse  | 29.99 | 50    |
    When I send an Avro message to topic "orders":
      | field       | value                                                    |
      | orderId     | ORD-2024-001                                            |
      | customerId  | 1                                                       |
      | items       | [{"productId":"P1","quantity":1,"price":999.99}]        |
      | totalAmount | 999.99                                                  |
      | timestamp   | 1640995200000                                           |
    Then I should receive an Avro message on topic "order-confirmed" within 5 seconds
    And the database should have the following orders:
      | order_id    | customer_id | total_amount | status    |
      | ORD-2024-001| 1          | 999.99       | CONFIRMED |
```

### User Registration Flow

```gherkin
Feature: User Registration with Email Verification

  Background:
    Given the following Avro schema is registered for topic "user-registrations":
      """
      {
        "type": "record",
        "name": "UserRegistration",
        "fields": [
          {"name": "userId", "type": "string"},
          {"name": "email", "type": "string"},
          {"name": "firstName", "type": "string"},
          {"name": "lastName", "type": "string"},
          {"name": "registrationDate", "type": "long"}
        ]
      }
      """

  Scenario: Register new user with email verification
    Given the input topic "user-registrations" is empty
    And the output topic "email-verification" is empty
    When I send an Avro message to topic "user-registrations":
      | field            | value                |
      | userId           | USER-001            |
      | email            | newuser@example.com |
      | firstName        | John                |
      | lastName         | Doe                 |
      | registrationDate | 1640995200000       |
    Then I should receive an Avro message on topic "email-verification" within 3 seconds
    And the database should have the following users:
      | user_id  | email               | status      |
      | USER-001 | newuser@example.com | PENDING     |
```

## 🐛 Troubleshooting

### Common Issues

**1. Schema Registry Connection Issues**
```bash
# The framework uses MockSchemaRegistry automatically
# No external schema registry needed for testing
```

**2. Avro Serialization Errors**
```java
// Ensure proper field types in your Gherkin data tables
| field      | value   |
| amount     | 100.50  |  # Double
| quantity   | 5       |  # Integer
| active     | true    |  # Boolean
```

**3. Schema Evolution Problems**
```gherkin
# Always add new fields with default values
{"name": "newField", "type": ["null", "string"], "default": null}
```

### Debug Logging

Enable detailed logging:

```yaml
logging:
  level:
    com.github.lelong1304.kafka.testing: DEBUG
    io.confluent.kafka: DEBUG
    org.springframework.kafka: DEBUG
```

## 🤝 Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for your changes
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [Apache Kafka](https://kafka.apache.org/) - Distributed streaming platform
- [Apache Avro](https://avro.apache.org/) - Data serialization system
- [Confluent](https://confluent.io/) - Kafka ecosystem tools
- [Cucumber](https://cucumber.io/) - BDD testing framework
- [Spring Boot](https://spring.io/projects/spring-boot) - Application framework
- [Testcontainers](https://testcontainers.org/) - Integration testing

---

**Happy Testing! 🎉**

For more examples and detailed documentation, check out the [examples](src/test/java/com/github/lelong1304/kafka/testing/examples/) directory.