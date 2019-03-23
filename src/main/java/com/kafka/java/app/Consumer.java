package com.kafka.java.app;

import com.kafka.java.CSVWriter.Writer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import com.kafka.java.properties.ConsumerProperties;

public class Consumer {

    public static void main(String[] args) {
        //Logger
        Logger logger = LoggerFactory.getLogger(Consumer.class);
        //bootstrap properties
        String bootstrapServers = "127.0.0.1:9092";
        //GroupID
        String groupId = "my_first_app";


        //topic to read from
        String topic = "first_topic";

        /*Consumer properties
        Properties properties = new Properties();

        //Custom properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // OFFSETS->" earliest/latest/none"
        */

        //create consumer
        KafkaConsumer<String, String> consumer =
                new ConsumerProperties().getKafkaConsumer(bootstrapServers,groupId);

        //Subscribe consumer to all the topic
        //consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));

        /*Assign kafka consumer for particular partition
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        long offsetToReadFrom = 15L;
        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);
        */



        while(true){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record:records){
                logger.info("Key: "+record.key()+"\t Value: "+record.value());
                logger.info("Partition: "+record.partition()+"\t Offset: "+record.offset());

                Writer writer = new Writer();
                writer.writeCSV(record.value());
            }
        }
    }
}
