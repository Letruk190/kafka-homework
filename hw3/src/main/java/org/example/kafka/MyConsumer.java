package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class MyConsumer {
    private static final String SERVER_CONFIG = "localhost:9091";
    private static final String OFFSET_RESET_CONFIG = "earliest";
    private static final String LEVEL_CONFIG = "read_committed";
    private static final String GROUP_ID_CONFIG = "mygroup";

    public static void main(String[] args) {

        var props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_CONFIG);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, LEVEL_CONFIG);

        ConsumerRecords<String, String> records;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            System.out.println("Consumer started");

            consumer.subscribe(List.of(MyProducer.TEST_1, MyProducer.TEST_2));
            while (!(records = consumer.poll(Duration.ofSeconds(20))).isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("%s -- %s, %s", record.topic(), record.key(), record.value());
                }
            }
        }
    }

}
