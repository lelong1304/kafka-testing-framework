Feature: Basic Kafka Message Testing

  Scenario: Send and receive a simple message
    Given the input topic "input-topic" is empty
    And the output topic "output-topic" is empty
    When I send a text message "Hello Kafka" to topic "input-topic"
    Then I should receive a message on topic "input-topic" within 5 seconds
    And the message should contain the text "Hello Kafka"

  Scenario: Send and receive JSON messages
    Given the input topic "input-topic" is empty
    When I send a message to topic "input-topic":
      | field1 | field2 |
      | value1 | value2 |
    Then I should receive a message on topic "input-topic" within 5 seconds
    And the message field "field1" should be "value1"
