package com.github.lelong1304.kafka.testing.framework;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * JUnit 5 extension that starts a Testcontainers Kafka and exposes bootstrap servers
 * and a mock schema registry for testing.
 */
public class KafkaTestEnvironment implements BeforeAllCallback, AfterAllCallback {

    private static final Map<String, KafkaContainer> KAFKA_CONTAINERS = new ConcurrentHashMap<>();
    private static final Map<String, MockSchemaRegistry> SCHEMA_REGISTRIES = new ConcurrentHashMap<>();
    
    private String bootstrapServers;
    private MockSchemaRegistry mockSchemaRegistry;
    private KafkaContainer kafkaContainer;

    @Override
    public void beforeAll(ExtensionContext context) {
        String contextId = context.getUniqueId();
        
        // Create and start Kafka container if not already started
        kafkaContainer = KAFKA_CONTAINERS.computeIfAbsent(contextId, k -> {
            KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
            container.start();
            return container;
        });
        
        bootstrapServers = kafkaContainer.getBootstrapServers();
        
        // Create mock schema registry
        mockSchemaRegistry = SCHEMA_REGISTRIES.computeIfAbsent(contextId, k -> new MockSchemaRegistry());
        
        // Store in context for access by tests
        context.getStore(ExtensionContext.Namespace.GLOBAL).put("bootstrapServers", bootstrapServers);
        context.getStore(ExtensionContext.Namespace.GLOBAL).put("mockSchemaRegistry", mockSchemaRegistry);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        String contextId = context.getUniqueId();
        
        // Clean up
        KafkaContainer container = KAFKA_CONTAINERS.remove(contextId);
        if (container != null) {
            container.stop();
        }
        
        MockSchemaRegistry registry = SCHEMA_REGISTRIES.remove(contextId);
        if (registry != null) {
            registry.clear();
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public MockSchemaRegistry getMockSchemaRegistry() {
        return mockSchemaRegistry;
    }
}
