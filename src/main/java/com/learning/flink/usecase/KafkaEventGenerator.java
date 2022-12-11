package com.learning.flink.usecase;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaEventGenerator implements Runnable {

    private static final String ANSI_RED = "\033[0;31m";
    public static final String ANSI_PURPLE = "\033[0;35m";

    public static void main(String[] args) {
        KafkaEventGenerator kafkaEventGenerator = new KafkaEventGenerator();
        kafkaEventGenerator.run();
    }

    @Override
    public void run() {
        try {
            // Setup Kafka Client
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "localhost:9092");
            kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> myProducer = new KafkaProducer<String, String>(kafkaProps);

            // List of users
            List<String> usersList = new ArrayList<String>();
            usersList.add("Hamza");
            usersList.add("Maaz");
            usersList.add("Shaaz");

            // List of actions
            List<String> actionsList = new ArrayList<>();
            actionsList.add("Login");
            actionsList.add("Logout");
            actionsList.add("View Video");
            actionsList.add("View Link");
            actionsList.add("View Review");

            Random random = new Random();

            for (int i=0; i<100; i++) {
                String timestamp = String.valueOf(System.currentTimeMillis());
                String user = usersList.get(random.nextInt(usersList.size()));
                String action = actionsList.get(random.nextInt(actionsList.size()));
                String[] record = {timestamp, user, action};

                // Produce record in Kafka
                ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>(
                        "flink.kafka.browser.source",
                        timestamp,
                        String.join(",", record)
                );

                RecordMetadata recordMetadata = myProducer.send(kafkaRecord).get();

                System.out.println(ANSI_PURPLE + "Kafka Stream Generator : Sending Event : " + String.join(",", record) + ANSI_RED);

                // Sleep for a random time (1 - 3 seconds) before the next record
                Thread.sleep(random.nextInt(2000) + 1);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
