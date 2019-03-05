package com.kafka.java.app;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutionException;
import com.kafka.java.properties.ProducerProperties;

public class Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //bootstrap properties
        String bootstrapServers = "127.0.0.1:9092";
        //Logger
        final Logger logger = LoggerFactory.getLogger(Producer.class);
        /*Producer properties
        Properties properties = new Properties();

        //Custom properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        */

        //Kafka producer
        KafkaProducer<String, String> kafkaProducer =
                new ProducerProperties().getKafkaProducer(bootstrapServers);

        for(int index = 1; index<11;index++) {

            //Record producer
            String topic = "first_topic";
            String value="MESSAGE\t"+index+"\t from java.app ";
            String key = "id_"+Integer.toString(index);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: "+key);

            //Without callback
            /*
             * kafkaProducer.send(record);
             */

            //With callback
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes if successfully sent.
                    if (e == null) {
                        logger.info("Metadata recieved. \n" +
                                "\nTopic:\t" + recordMetadata.topic() +
                                "\nPartition:\t" + recordMetadata.partition() +
                                "\nOffset\t" + recordMetadata.offset() +
                                "\nTimestamp:\t" + recordMetadata.timestamp());
                    } else {
                        logger.error("ERROR PRODUCING", e);
                    }
                }
            }).get(); //.get() makes send() synchronous.

            kafkaProducer.flush();
        }
    }
}
