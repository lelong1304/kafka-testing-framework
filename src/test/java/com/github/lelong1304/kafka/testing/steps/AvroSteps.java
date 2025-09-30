package com.github.lelong1304.kafka.testing.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lelong1304.kafka.testing.avro.AvroSchemaManager;
import com.github.lelong1304.kafka.testing.kafka.AvroMessageCollector;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AvroSteps {

    @Autowired
    private KafkaTemplate<String, Object> avroKafkaTemplate;
    
    @Autowired
    private AvroMessageCollector avroMessageCollector;
    
    @Autowired
    private AvroSchemaManager schemaManager;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Given("the following Avro schema is registered for topic {string}:")
    public void theFollowingAvroSchemaIsRegisteredForTopic(String topic, String schemaJson) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaJson);
        schemaManager.registerSchema(topic + "-value", schema);
    }

    @When("I send an Avro message to topic {string}:")
    public void iSendAnAvroMessageToTopic(String topic, DataTable dataTable) throws Exception {
        Map<String, String> messageData = dataTable.asMaps(String.class, String.class).get(0);
        
        Schema schema = schemaManager.getLatestSchema(topic + "-value");
        GenericRecord record = new GenericData.Record(schema);
        
        for (Map.Entry<String, String> entry : messageData.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            
            Schema.Field field = schema.getField(fieldName);
            if (field != null) {
                Object convertedValue = convertValue(fieldValue, field.schema());
                record.put(fieldName, convertedValue);
            }
        }
        
        avroKafkaTemplate.send(topic, record);
        avroKafkaTemplate.flush();
    }

    @When("I send an Avro message with key {string} to topic {string}:")
    public void iSendAnAvroMessageWithKeyToTopic(String key, String topic, DataTable dataTable) throws Exception {
        Map<String, String> messageData = dataTable.asMaps(String.class, String.class).get(0);
        
        Schema schema = schemaManager.getLatestSchema(topic + "-value");
        GenericRecord record = new GenericData.Record(schema);
        
        for (Map.Entry<String, String> entry : messageData.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            
            Schema.Field field = schema.getField(fieldName);
            if (field != null) {
                Object convertedValue = convertValue(fieldValue, field.schema());
                record.put(fieldName, convertedValue);
            }
        }
        
        avroKafkaTemplate.send(topic, key, record);
        avroKafkaTemplate.flush();
    }

    @Then("I should receive an Avro message on topic {string} within {int} seconds")
    public void iShouldReceiveAnAvroMessageOnTopicWithinSeconds(String topic, int timeoutSeconds) {
        avroMessageCollector.startCollecting(topic);
        
        await()
            .atMost(timeoutSeconds, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> avroMessageCollector.getMessages(topic).size() > 0);
    }

    @And("the Avro message should have:")
    public void theAvroMessageShouldHave(DataTable dataTable) {
        GenericRecord lastMessage = avroMessageCollector.getLastMessage();
        assertThat(lastMessage).isNotNull();
        
        Map<String, String> expectedFields = dataTable.asMaps(String.class, String.class).get(0);
        
        for (Map.Entry<String, String> entry : expectedFields.entrySet()) {
            String fieldName = entry.getKey();
            String expectedValue = entry.getValue();
            
            Object actualValue = lastMessage.get(fieldName);
            assertThat(actualValue).isNotNull();
            assertThat(actualValue.toString()).isEqualTo(expectedValue);
        }
    }

    @And("the Avro message field {string} should be {string}")
    public void theAvroMessageFieldShouldBe(String fieldName, String expectedValue) {
        GenericRecord lastMessage = avroMessageCollector.getLastMessage();
        assertThat(lastMessage).isNotNull();
        
        Object actualValue = lastMessage.get(fieldName);
        assertThat(actualValue).isNotNull();
        assertThat(actualValue.toString()).isEqualTo(expectedValue);
    }

    @Then("I should receive exactly {int} Avro message(s) on topic {string} within {int} seconds")
    public void iShouldReceiveExactlyAvroMessagesOnTopicWithinSeconds(int expectedCount, String topic, int timeoutSeconds) {
        avroMessageCollector.startCollecting(topic);
        
        await()
            .atMost(timeoutSeconds, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> avroMessageCollector.getMessages(topic).size() == expectedCount);
    }

    private Object convertValue(String value, Schema schema) throws Exception {
        Schema.Type type = schema.getType();
        
        switch (type) {
            case STRING:
                return value;
            case INT:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case ARRAY:
                // Handle JSON array strings
                if (value.startsWith("[") && value.endsWith("]")) {
                    List<?> list = objectMapper.readValue(value, List.class);
                    return list;
                }
                return value;
            case UNION:
                // Handle union types (typically nullable fields)
                for (Schema unionSchema : schema.getTypes()) {
                    if (unionSchema.getType() != Schema.Type.NULL) {
                        return convertValue(value, unionSchema);
                    }
                }
                return value;
            default:
                return value;
        }
    }
}