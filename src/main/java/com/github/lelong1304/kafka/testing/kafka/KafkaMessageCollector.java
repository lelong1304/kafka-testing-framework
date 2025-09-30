package com.github.lelong1304.kafka.testing.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Collector for standard Kafka messages (String key/value).
 * Manages message collection from topics for testing purposes.
 */
@Component
public class KafkaMessageCollector {

    @Autowired
    private ConsumerFactory<String, String> stringConsumerFactory;

    private final Map<String, List<ConsumerRecord<String, String>>> collectedMessages = new ConcurrentHashMap<>();
    private final Map<String, KafkaConsumer<String, String>> activeConsumers = new ConcurrentHashMap<>();
    private String lastMessage;

    public void startCollecting(String topic) {
        if (!activeConsumers.containsKey(topic)) {
            @SuppressWarnings("unchecked")
            KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) stringConsumerFactory.createConsumer();
            consumer.subscribe(Collections.singletonList(topic));
            activeConsumers.put(topic, consumer);
            collectedMessages.put(topic, new CopyOnWriteArrayList<>());

            // Start polling in a separate thread
            Thread pollingThread = new Thread(() -> {
                try {
                    while (activeConsumers.containsKey(topic)) {
                        consumer.poll(Duration.ofMillis(100))
                            .forEach(record -> {
                                collectedMessages.get(topic).add(record);
                                lastMessage = record.value();
                            });
                    }
                } catch (Exception e) {
                    // Consumer was closed
                }
            });
            pollingThread.setDaemon(true);
            pollingThread.start();
        }
    }

    public void stopCollecting(String topic) {
        KafkaConsumer<String, String> consumer = activeConsumers.remove(topic);
        if (consumer != null) {
            consumer.close();
        }
    }

    public List<ConsumerRecord<String, String>> getMessages(String topic) {
        return collectedMessages.getOrDefault(topic, Collections.emptyList());
    }

    public void clearMessages(String topic) {
        List<ConsumerRecord<String, String>> messages = collectedMessages.get(topic);
        if (messages != null) {
            messages.clear();
        }
        lastMessage = null;
    }

    public String getLastMessage() {
        return lastMessage;
    }

    @PreDestroy
    public void cleanup() {
        activeConsumers.keySet().forEach(this::stopCollecting);
    }
}
