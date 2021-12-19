package com.github.saskarad.elasticsearch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static KafkaConsumer<String,String> consumer;
    public static KafkaConsumer<String,String > createConsumer(String topic){
        if (consumer == null){
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"elastic-twitter-app");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

            // create the producer
            consumer = new KafkaConsumer<String, String>(properties);
        }
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
