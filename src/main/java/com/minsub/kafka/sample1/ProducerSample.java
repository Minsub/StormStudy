package com.minsub.kafka.sample1;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by jiminsub on 2016. 11. 5..
 */
public class ProducerSample {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        KeyedMessage<String, String> message = new KeyedMessage<String, String>("test", "Hello, World!");
        producer.send(message);


        // multiple sending
        List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
        for (int i = 0; i < 10; i++) {
            messages.add(new KeyedMessage<String, String>("test", "Hello, World!"));
        }
        producer.send(messages);


        producer.close();

        System.out.println("END ProducerSample");
    }
}
