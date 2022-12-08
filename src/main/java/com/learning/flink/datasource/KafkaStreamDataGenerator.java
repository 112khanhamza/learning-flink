package com.learning.flink.datasource;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.util.*;

public class KafkaStreamDataGenerator implements Runnable {

    private static final String ANSI_BLUE = "\033[0;34m";
    private static final String ANSI_RED = "\033[0;31m";
    public static final String ANSI_PURPLE = "\033[0;35m";

    @Override
    public void run() {
        try {
            // Setup Kafka Client
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "localhost:9092");
            kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> myProducer = new KafkaProducer<String, String>(kafkaProps);

            // Define list of users
            List<String> appUser = new ArrayList<>();
            appUser.add("Hamza");
            appUser.add("Maaz");
            appUser.add("Talha");
            appUser.add("Shaaz");

            // Define list of application operations
            List<String> appOps = new ArrayList<>();
            appOps.add("Create");
            appOps.add("Read");
            appOps.add("Update");
            appOps.add("Delete");

            // Define list of application entities
            List<String> appEntity = new ArrayList<>();
            appEntity.add("Customer");
            appEntity.add("SalesRep");

            // Define a random number generator
            Random random = new Random();

            // Generate 100 random records 1 per each file
            for (int i=0; i<100; i++) {

                // Get current_timestamp in String
                String currentTime = String.valueOf(System.currentTimeMillis());
                String timestamp = currentTime;
                // Generate a random user
                String user = appUser.get(random.nextInt(appUser.size()));
                // Generate a random operation
                String operation = appOps.get(random.nextInt(appOps.size()));
                // Generate a random entity
                String entity = appEntity.get(random.nextInt(appEntity.size()));
                // Generate a random duration for the operation
                String duration = String.valueOf(random.nextInt(10) + 1);
                // Generate a random value for number of changes
                String changeCount = String.valueOf(random.nextInt(4) + 1);

                // Create a CSV text array
                String[] csvText = {String.valueOf(i), user, entity, operation, timestamp, duration, changeCount};

                String recKey = String.valueOf(currentTime);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("flink.kafka.streaming.source",
                                                            recKey,
                                                            String.join(",", csvText));

                RecordMetadata rmd = myProducer.send(record).get();

                System.out.println(ANSI_PURPLE + "Kafka Stream Generator : Sending Event : " + String.join(",", csvText) + ANSI_RED);

                // Sleep for a random time (1 - 3 seconds) before the next record
                Thread.sleep(random.nextInt(2000) + 1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
