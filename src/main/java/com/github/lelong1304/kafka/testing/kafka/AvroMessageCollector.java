package com.github.lelong1304.kafka.testing.kafka;

import org.apache.avro.generic.GenericRecord;
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

@Component
public class AvroMessageCollector {

    @Autowired
    private ConsumerFactory<String, Object> avroConsumerFactory;

    private final Map<String, List<ConsumerRecord<String, Object>>> collectedMessages = new ConcurrentHashMap<>();
    private final Map<String, KafkaConsumer<String, Object>> activeConsumers = new ConcurrentHashMap<>();
    private GenericRecord lastMessage;

    public void startCollecting(String topic) {
        if (!activeConsumers.containsKey(topic)) {
            @SuppressWarnings("unchecked")
            KafkaConsumer<String, Object> consumer = (KafkaConsumer<String, Object>) avroConsumerFactory.createConsumer();
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
                                if (record.value() instanceof GenericRecord) {
                                    lastMessage = (GenericRecord) record.value();
                                }
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
        KafkaConsumer<String, Object> consumer = activeConsumers.remove(topic);
        if (consumer != null) {
            consumer.close();
        }
    }

    public List<ConsumerRecord<String, Object>> getMessages(String topic) {
        return collectedMessages.getOrDefault(topic, Collections.emptyList());
    }

    public void clearMessages(String topic) {
        List<ConsumerRecord<String, Object>> messages = collectedMessages.get(topic);
        if (messages != null) {
            messages.clear();
        }
        lastMessage = null;
    }

    public GenericRecord getLastMessage() {
        return lastMessage;
    }

    @PreDestroy
    public void cleanup() {
        activeConsumers.keySet().forEach(this::stopCollecting);
    }
}