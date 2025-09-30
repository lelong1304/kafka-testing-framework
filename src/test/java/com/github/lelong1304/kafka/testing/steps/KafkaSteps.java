package com.github.lelong1304.kafka.testing.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lelong1304.kafka.testing.kafka.KafkaMessageCollector;
import com.github.lelong1304.kafka.testing.utils.MessageMatchers;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Cucumber step definitions for standard Kafka operations.
 * Handles JSON and string message production/consumption.
 */
public class KafkaSteps {

    @Autowired
    private KafkaTemplate<String, String> stringKafkaTemplate;

    @Autowired
    private KafkaMessageCollector kafkaMessageCollector;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Given("the input topic {string} is empty")
    public void theInputTopicIsEmpty(String topic) {
        kafkaMessageCollector.startCollecting(topic);
        kafkaMessageCollector.clearMessages(topic);
    }

    @Given("the output topic {string} is empty")
    public void theOutputTopicIsEmpty(String topic) {
        kafkaMessageCollector.startCollecting(topic);
        kafkaMessageCollector.clearMessages(topic);
    }

    @When("I send a message to topic {string}:")
    public void iSendAMessageToTopic(String topic, DataTable dataTable) throws Exception {
        Map<String, String> messageData = dataTable.asMaps(String.class, String.class).get(0);
        String jsonMessage = objectMapper.writeValueAsString(messageData);
        stringKafkaTemplate.send(topic, jsonMessage);
        stringKafkaTemplate.flush();
    }

    @When("I send a message with key {string} to topic {string}:")
    public void iSendAMessageWithKeyToTopic(String key, String topic, DataTable dataTable) throws Exception {
        Map<String, String> messageData = dataTable.asMaps(String.class, String.class).get(0);
        String jsonMessage = objectMapper.writeValueAsString(messageData);
        stringKafkaTemplate.send(topic, key, jsonMessage);
        stringKafkaTemplate.flush();
    }

    @When("I send a JSON message to topic {string}:")
    public void iSendAJsonMessageToTopic(String topic, String jsonMessage) {
        stringKafkaTemplate.send(topic, jsonMessage);
        stringKafkaTemplate.flush();
    }

    @When("I send a text message {string} to topic {string}")
    public void iSendATextMessageToTopic(String message, String topic) {
        stringKafkaTemplate.send(topic, message);
        stringKafkaTemplate.flush();
    }

    @Then("I should receive a message on topic {string} within {int} seconds")
    public void iShouldReceiveAMessageOnTopicWithinSeconds(String topic, int timeoutSeconds) {
        kafkaMessageCollector.startCollecting(topic);
        
        await()
            .atMost(timeoutSeconds, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> kafkaMessageCollector.getMessages(topic).size() > 0);
    }

    @Then("I should receive exactly {int} message(s) on topic {string} within {int} seconds")
    public void iShouldReceiveExactlyMessagesOnTopicWithinSeconds(int expectedCount, String topic, int timeoutSeconds) {
        kafkaMessageCollector.startCollecting(topic);
        
        await()
            .atMost(timeoutSeconds, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> kafkaMessageCollector.getMessages(topic).size() == expectedCount);
    }

    @Then("the message should contain:")
    public void theMessageShouldContain(DataTable dataTable) {
        String lastMessage = kafkaMessageCollector.getLastMessage();
        assertThat(lastMessage).isNotNull();
        
        Map<String, String> expectedFields = dataTable.asMaps(String.class, String.class).get(0);
        assertThat(MessageMatchers.messageContains(lastMessage, expectedFields)).isTrue();
    }

    @Then("the message should contain the text {string}")
    public void theMessageShouldContainTheText(String text) {
        String lastMessage = kafkaMessageCollector.getLastMessage();
        assertThat(lastMessage).isNotNull();
        assertThat(MessageMatchers.messageContainsText(lastMessage, text)).isTrue();
    }

    @Then("the message field {string} should be {string}")
    public void theMessageFieldShouldBe(String fieldName, String expectedValue) throws Exception {
        String lastMessage = kafkaMessageCollector.getLastMessage();
        assertThat(lastMessage).isNotNull();
        
        Map<String, String> expected = new HashMap<>();
        expected.put(fieldName, expectedValue);
        assertThat(MessageMatchers.messageContains(lastMessage, expected)).isTrue();
    }

    @Then("no messages should be on topic {string}")
    public void noMessagesShouldBeOnTopic(String topic) {
        kafkaMessageCollector.startCollecting(topic);
        List<ConsumerRecord<String, String>> messages = kafkaMessageCollector.getMessages(topic);
        assertThat(messages).isEmpty();
    }
}
