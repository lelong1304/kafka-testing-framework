package com.github.lelong1304.kafka.testing.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.Map;

/**
 * Utility class for matching message content in tests.
 * Provides flexible matching for JSON and string content.
 */
public class MessageMatchers {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Check if a message contains all specified key-value pairs
     */
    public static boolean messageContains(String message, Map<String, String> expectedFields) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            
            for (Map.Entry<String, String> entry : expectedFields.entrySet()) {
                String key = entry.getKey();
                String expectedValue = entry.getValue();
                
                JsonNode fieldNode = getNestedField(jsonNode, key);
                if (fieldNode == null || fieldNode.isNull()) {
                    return false;
                }
                
                String actualValue = fieldNode.asText();
                if (!actualValue.equals(expectedValue)) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            // If not JSON, do simple string matching
            return message.contains(expectedFields.toString());
        }
    }

    /**
     * Get a nested field from JSON using dot notation (e.g., "user.name")
     */
    private static JsonNode getNestedField(JsonNode node, String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        JsonNode current = node;
        
        for (String part : parts) {
            if (current == null) {
                return null;
            }
            current = current.get(part);
        }
        
        return current;
    }

    /**
     * Check if a message contains a specific text substring
     */
    public static boolean messageContainsText(String message, String text) {
        return message != null && message.contains(text);
    }

    /**
     * Check if messages are equal (JSON-aware comparison)
     */
    public static boolean messagesEqual(String message1, String message2) {
        try {
            JsonNode node1 = objectMapper.readTree(message1);
            JsonNode node2 = objectMapper.readTree(message2);
            return node1.equals(node2);
        } catch (Exception e) {
            // If not JSON, do simple string comparison
            return message1.equals(message2);
        }
    }

    /**
     * Partial match - check if message contains a subset of fields
     */
    public static boolean partialMatch(String message, String partialJson) {
        try {
            JsonNode messageNode = objectMapper.readTree(message);
            JsonNode partialNode = objectMapper.readTree(partialJson);
            
            return containsAllFields(messageNode, partialNode);
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean containsAllFields(JsonNode messageNode, JsonNode expectedNode) {
        Iterator<Map.Entry<String, JsonNode>> fields = expectedNode.fields();
        
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();
            JsonNode expectedValue = field.getValue();
            
            JsonNode actualValue = messageNode.get(key);
            if (actualValue == null) {
                return false;
            }
            
            if (expectedValue.isObject()) {
                if (!containsAllFields(actualValue, expectedValue)) {
                    return false;
                }
            } else if (!actualValue.equals(expectedValue)) {
                return false;
            }
        }
        
        return true;
    }
}
