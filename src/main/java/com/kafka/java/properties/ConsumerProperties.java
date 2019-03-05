package com.kafka.java.properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;

public class ConsumerProperties {

    public KafkaConsumer<String, String> getKafkaConsumer(String bootstrapServers, String groupId)
    {
        //Consumer properties
        Properties properties = new Properties();

        //Custom properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // OFFSETS->" earliest/latest/none"

        //create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);

        return consumer;
    }

}
