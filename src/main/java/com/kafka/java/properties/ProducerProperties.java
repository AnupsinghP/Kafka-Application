package com.kafka.java.properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerProperties {

    public KafkaProducer<String, String> getKafkaProducer(String bootstrapServers){
        //Producer properties
        Properties properties = new Properties();

        //Custom properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Kafka producer
        KafkaProducer<String, String> kafkaProducer =
                new KafkaProducer<String, String>(properties);

        return kafkaProducer;
    }
}
