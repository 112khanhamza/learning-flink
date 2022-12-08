package com.learning.flink.windowing;

import com.learning.flink.common.Utils;
import com.learning.flink.datasource.KafkaStreamDataGenerator;
import com.learning.flink.datastreamapi.AuditTrail;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class WindowingOperations {
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
            DataStream<String> auditTrailStr = env.addSource(kafkaConsumer);

            // Convert each record into an Object
            DataStream<AuditTrail> auditTrailObj = auditTrailStr.map(new MapFunction<String, AuditTrail>() {
                @Override
                public AuditTrail map(String s) throws Exception {
                    System.out.println("--- Received Record : " + s);
                    return new AuditTrail(s);
                }
            });

            /**************************************************************************
             *                          Use Sliding Windows
             ************************************************************************/

            // Compute the count of events, min timestamp and max timestamp
            // for a sliding window interval of 10 seconds, sliding by 5 seconds
//            DataStream<Tuple4<String, Integer, Long, Long>> slidingSummary = auditTrailObj.map(i ->
//                            new Tuple4<String, Integer, Long, Long>(
//                            String.valueOf(System.currentTimeMillis()), // Current Time
//                            1, // Count each record
//                            i.getTimestamp(), // Min timestamp
//                            i.getTimestamp())) // Max timestamp
//
//                    .returns(Types.TUPLE(Types.STRING,
//                            Types.INT,
//                            Types.LONG,
//                            Types.LONG))
//
//                    .timeWindowAll(Time.seconds(10), // Window size
//                                    Time.seconds(5)) // Slide by 5
//
//                    .reduce((x, y) ->
//                        new Tuple4<String, Integer, Long, Long>(
//                                x.f0,
//                                x.f1 + y.f1,
//                                Math.min(x.f2, y.f2),
//                                Math.max(x.f3, y.f3)
//                        ));

            /**************************************************************************
             *                          Use Session Windows
             ************************************************************************/

            // Execute the same example as before using Session Windows
            // Partition by User and use a window gap of 5 seconds
            DataStream<Tuple4<String, Integer, Long, Long>> sessionSummary = auditTrailObj.map(i ->
                    new Tuple4<String, Integer, Long, Long>(
                            i.getUser(), // Get user
                            1, // Count each record
                            i.getTimestamp(), // Min timestamp
                            i.getTimestamp() // Max timestamp
                    ))

                    .returns(Types.TUPLE(Types.STRING,
                            Types.INT,
                            Types.LONG,
                            Types.LONG))

                    .keyBy(0)

                    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                    .reduce((x, y) ->
                            new Tuple4<String, Integer, Long, Long>(
                                    x.f0,
                                    x.f1 + y.f1,
                                    Math.min(x.f2, y.f2),
                                    Math.max(x.f3, y.f3)
                            ));


            // Pretty print the tuples
            sessionSummary.map(new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {
                @Override
                public Object map(Tuple4<String, Integer, Long, Long> slidingSummary) throws Exception{
                    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

                    String minTime = format.format(new Date(Long.valueOf(slidingSummary.f2)));
                    String maxTime = format.format(new Date(Long.valueOf(slidingSummary.f3)));

                    System.out.println("Sliding Summary : "
                    + (new Date()).toString()
                    + " User : " + slidingSummary.f0
                    + " Start Time : " + minTime
                    + " End Time : " + maxTime
                    + " Count : " + slidingSummary.f1);

                    return null;
                }
            });

            /**************************************************************************
             *          Setup Kafka Data source and execute the Flink Pipeline
             ************************************************************************/

            // Start the kafka stream generator on a separate thread
            Utils.printHeader("String Kafka Data Generator...");
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            env.execute("Flink Windowing Example");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
