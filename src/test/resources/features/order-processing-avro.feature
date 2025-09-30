Feature: Order Processing Kafka Stream with Avro

  Background:
    Given the following Avro schema is registered for topic "orders":
      """
      {
        "type": "record",
        "name": "Order",
        "namespace": "com.example.orders",
        "fields": [
          {"name": "orderId", "type": "string"},
          {"name": "customerId", "type": "long"},
          {"name": "amount", "type": "double"},
          {"name": "items", "type": {"type": "array", "items": "string"}},
          {"name": "timestamp", "type": ["null", "long"], "default": null}
        ]
      }
      """
    And the following Avro schema is registered for topic "processed-orders":
      """
      {
        "type": "record",
        "name": "ProcessedOrder",
        "namespace": "com.example.orders",
        "fields": [
          {"name": "orderId", "type": "string"},
          {"name": "customerId", "type": "long"},
          {"name": "status", "type": "string"},
          {"name": "processedAt", "type": "long"},
          {"name": "totalAmount", "type": "double"}
        ]
      }
      """
    And the database has the following customers:
      | id | name     | email            |
      | 1  | John Doe | john@example.com |
      | 2  | Jane Doe | jane@example.com |

  Scenario: Process valid order message with Avro
    Given the input topic "orders" is empty
    And the output topic "processed-orders" is empty
    When I send an Avro message to topic "orders":
      | field      | value                    |
      | orderId    | ORDER-12345             |
      | customerId | 1                       |
      | amount     | 150.75                  |
      | items      | ["laptop", "mouse"]     |
      | timestamp  | 1640995200000           |
    Then I should receive an Avro message on topic "processed-orders" within 10 seconds
    And the Avro message should have:
      | field        | value       |
      | orderId      | ORDER-12345 |
      | customerId   | 1           |
      | status       | PROCESSED   |
      | totalAmount  | 150.75      |
    And the database should have the following orders:
      | order_id    | customer_id | amount | status    |
      | ORDER-12345 | 1          | 150.75 | PROCESSED |

  Scenario: Process order with invalid customer using Avro
    Given the input topic "orders" is empty
    And the output topic "error-orders" is empty
    And the following Avro schema is registered for topic "error-orders":
      """
      {
        "type": "record",
        "name": "ErrorOrder",
        "namespace": "com.example.orders",
        "fields": [
          {"name": "orderId", "type": "string"},
          {"name": "customerId", "type": "long"},
          {"name": "error", "type": "string"},
          {"name": "originalAmount", "type": "double"}
        ]
      }
      """
    When I send an Avro message to topic "orders":
      | field      | value           |
      | orderId    | ORDER-99999     |
      | customerId | 999             |
      | amount     | 75.50           |
      | items      | ["invalid"]     |
    Then I should receive an Avro message on topic "error-orders" within 5 seconds
    And the Avro message should have:
      | field          | value              |
      | orderId        | ORDER-99999        |
      | customerId     | 999                |
      | error          | Customer not found |
      | originalAmount | 75.50              |
    And the database table "orders" should be empty

  Scenario: Process multiple Avro orders with keys
    Given the input topic "orders" is empty
    And the output topic "processed-orders" is empty
    When I send an Avro message with key "customer-1" to topic "orders":
      | field      | value               |
      | orderId    | ORDER-001          |
      | customerId | 1                  |
      | amount     | 99.99              |
      | items      | ["book"]           |
    And I send an Avro message with key "customer-2" to topic "orders":
      | field      | value               |
      | orderId    | ORDER-002          |
      | customerId | 2                  |
      | amount     | 249.99             |
      | items      | ["phone", "case"]  |
    Then I should receive exactly 2 Avro messages on topic "processed-orders" within 15 seconds
    And the database table "orders" should have 2 rows

  Scenario: Verify Avro schema evolution compatibility
    Given the input topic "orders" is empty
    And the following Avro schema is registered for topic "orders":
      """
      {
        "type": "record",
        "name": "Order",
        "namespace": "com.example.orders",
        "fields": [
          {"name": "orderId", "type": "string"},
          {"name": "customerId", "type": "long"},
          {"name": "amount", "type": "double"},
          {"name": "items", "type": {"type": "array", "items": "string"}},
          {"name": "timestamp", "type": ["null", "long"], "default": null},
          {"name": "priority", "type": ["null", "string"], "default": null}
        ]
      }
      """
    When I send an Avro message to topic "orders":
      | field      | value           |
      | orderId    | ORDER-PRIORITY  |
      | customerId | 1               |
      | amount     | 500.00          |
      | items      | ["premium"]     |
      | priority   | HIGH            |
    Then I should receive an Avro message on topic "processed-orders" within 10 seconds
    And the Avro message field "orderId" should be "ORDER-PRIORITY"

  Scenario: Handle Avro union types and null values
    Given the input topic "orders" is empty
    When I send an Avro message to topic "orders":
      | field      | value           |
      | orderId    | ORDER-NULL-TEST |
      | customerId | 1               |
      | amount     | 25.00           |
      | items      | ["test"]        |
    Then I should receive an Avro message on topic "processed-orders" within 5 seconds
    And the Avro message should have:
      | field      | value           |
      | orderId    | ORDER-NULL-TEST |
      | status     | PROCESSED       |