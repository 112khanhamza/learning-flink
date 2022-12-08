package com.learning.flink.windowing;

import com.learning.flink.common.Utils;
import com.learning.flink.datasource.CsvFileGeneration;
import com.learning.flink.datasource.KafkaStreamDataGenerator;
import com.learning.flink.datastreamapi.AuditTrail;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class WindowJoin {
    public static void main(String[] args) {
        try {
            /********************************************************
             *          Setup Flink Streaming Environment
             *******************************************************/

            // Setup Flink Stream Environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /********************************************************
             *          Read Kafka Topic Stream into a DataStream
             *******************************************************/
            // Set Kafka Properties
            Properties kafkaProp = new Properties();
            kafkaProp.setProperty("bootstrap.servers", "localhost:9092");
            kafkaProp.setProperty("group.id", "flink.learn.realtime");

            // Setup a Kafka Consumer on Flink
            FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("flink.kafka.streaming.source", // topic name
                    new SimpleStringSchema(), // schema for data
                    kafkaProp); // connection properties

            // Setup only to read new messages
            kafkaConsumer.setStartFromLatest();

            // Create the data stream
            DataStream<String> auditTrailKafkaStr = env.addSource(kafkaConsumer);

            // Convert each record into an Object
            DataStream<AuditTrail> auditTrailKafkaObj = auditTrailKafkaStr.map(new MapFunction<String, AuditTrail>() {
                @Override
                public AuditTrail map(String s) throws Exception {
                    System.out.println("--- Received Record : " + s);
                    return new AuditTrail(s);
                }
            });

            /************************************************************************
             *                READ CSV FILE TO STREAM DATA TO A DATASTREAM
             ***********************************************************************/

            // Define the data directory to monitor new files
            String dir = "data/raw_audit_trail";

            // Define the text input format based on the directory
            TextInputFormat auditFormat = new TextInputFormat(new Path(dir));

            // Create a DataStream based on that directory
            DataStream<String> auditTrailCsvStr = env.readFile(auditFormat,
                    dir, // Directory to monitor
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    1000);

            // Convert each record in the stream to an Object
            DataStream<AuditTrail> auditTrailCsvObj = auditTrailCsvStr.map(new MapFunction<String, AuditTrail>() {
                @Override
                public AuditTrail map(String auditStr) throws Exception {
                    System.out.println("---Received Record : " + auditStr);
                    return new AuditTrail(auditStr);
                }
            });

            /****************************************************************************
             *                  Join both streams based on the same window
             ****************************************************************************/
            DataStream<Tuple2<String, Integer>> joinStream = auditTrailCsvObj.join(auditTrailKafkaObj) // Join the two streams
                    // Where to select join column in First Stream
                    .where(new KeySelector<AuditTrail, String>() {
                        @Override
                        public String getKey(AuditTrail auditTrail) throws Exception {
                            return auditTrail.getUser();
                        }
                    })
                    // EqualTo to select join column in Second Stream
                    .equalTo(new KeySelector<AuditTrail, String>() {
                        @Override
                        public String getKey(AuditTrail auditTrail) throws Exception {
                            return auditTrail.getUser();
                        }
                    })
                    // Create a Tumbling window of 5 seconds
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                    // Apply join function. Will be called for combination of each matched record
                    .apply(new JoinFunction<AuditTrail, AuditTrail, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> join(AuditTrail csvTrail, AuditTrail kafkaTrail) throws Exception {
                            return new Tuple2<String, Integer>(csvTrail.getUser(), 1);
                        }
                    });

            joinStream.print();

            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/
            //Start the File Stream generator on a separate thread
            Utils.printHeader("Starting File Data Generator...");
            Thread genThread = new Thread(new CsvFileGeneration());
            genThread.start();

            //Start the Kafka Stream generator on a separate thread
            Utils.printHeader("Starting Kafka Data Generator...");
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            // execute the streaming pipeline
            env.execute("Flink Streaming Window Joins Example");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
