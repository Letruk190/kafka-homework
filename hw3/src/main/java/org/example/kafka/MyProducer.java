package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {
    public static final String TEST_1 = "test1";
    public static final String TEST_2 = "test2";
    private static final String SERVER_CONFIG = "localhost:9091";
    private static final String KEY_SERIALIZER_CLASS_CONFIG_VALUE = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String VALUE_SERIALIZER_CLASS_CONFIG_VALUE = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String TRANSACTIONAL_ID = "hw3";
    private static final String SUCCESS_KEY = "success key: ";
    private static final String FAILURE_KEY = "failure key: ";
    private static final String SUCCESS_VALUE = "success value: ";
    private static final String FAILURE_VALUE = "failure value: ";

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG_VALUE);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG_VALUE);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID);
        var producer = new KafkaProducer<>(props);

        producer.initTransactions();
        producer.beginTransaction();
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(TEST_1, SUCCESS_KEY + i, TEST_1 + " " + SUCCESS_VALUE + i));
            producer.send(new ProducerRecord<>(TEST_2, SUCCESS_KEY + i, TEST_2 + " " + SUCCESS_VALUE + i));
        }
        producer.commitTransaction();

        producer.beginTransaction();
        for (int i = 0; i < 2; i++) {
            producer.send(new ProducerRecord<>(TEST_1, FAILURE_KEY + i, TEST_1 + " " + FAILURE_VALUE + i));
            producer.send(new ProducerRecord<>(TEST_2, FAILURE_KEY + i, TEST_2 + " " + FAILURE_VALUE + i));
        }
        Thread.sleep(200);
        producer.abortTransaction();

        producer.close();
    }
}
