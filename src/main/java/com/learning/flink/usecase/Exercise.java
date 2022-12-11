package com.learning.flink.usecase;

import com.learning.flink.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Exercise {
    public static void main(String[] args) {

        final String ANSI_BLUE = "\033[0;34m";

        try {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            // Set Kafka Properties
            Properties kafkaProp = new Properties();
            kafkaProp.setProperty("bootstrap.servers", "localhost:9092");
            kafkaProp.setProperty("group.id", "flink.kafka.browser.source");

            // Setup a Kafka Consumer on Flink
            FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("flink.kafka.browser.source", // topic name
                    new SimpleStringSchema(), // schema for data
                    kafkaProp); // connection properties

            // Setup only to read new messages
            kafkaConsumer.setStartFromLatest();

            // Reading Data from Kafka as String
            DataStream<String> readStreamStr = env.addSource(kafkaConsumer);

            // Convert to Stream of Object
            DataStream<User> userDataStream = readStreamStr.map(new MapFunction<String, User>() {
                @Override
                public User map(String s) throws Exception {
                    System.out.println("--- Received Record : " + s);
                    return new User(s);
                }
            });

            // Convert Stream to Tuple
            DataStream<Tuple3<String, String, Long>> userTupleStream = readStreamStr.map(new MapFunction<String, Tuple3<String, String, Long>>() {
                @Override
                public Tuple3<String, String, Long> map(String s) throws Exception {
                    System.out.println("--- Received Record : " + s);
                    String[] userArr = s.replace("\"", "").split(",");

                    return new Tuple3<String, String, Long>(
                            userArr[1],
                            userArr[2],
                            Long.valueOf(userArr[0])
                    );
                }
            });

            // Compute 10-seconds summaries - by user, by action, total counts
            DataStream<Tuple3<String, String, Integer>> userSummaries = userDataStream
                    // Extract User, Action, and Count 1
                    .map(i -> new Tuple3<String, String, Integer>(
                            i.getName(),
                            i.getAction(),
                            1
                    ))

                    .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))

                    // By user and action
                    .keyBy(0, 1)

                    // 10 seconds window
                    .timeWindow(Time.seconds(10))

                    // Sum the counts
                    .reduce((x, y) -> new Tuple3<String, String, Integer>(x.f0, x.f1, x.f2 + y.f2))
                    ;

            // Pretty Print the summary
            userSummaries.map(new MapFunction<Tuple3<String, String, Integer>, Object>() {
                @Override
                public Object map(Tuple3<String, String, Integer> summary) throws Exception {
                    System.out.println(ANSI_BLUE + "User Action Summary : " +
                            " User : " + summary.f0 +
                            " Action : " + summary.f1 +
                            " Count : " + summary.f2);
                    return null;
                }
            });

            // Compute duration for each action for each user
            DataStream<Tuple3<String, String, Long>> userActionDuration = userTupleStream

                    .keyBy(0) // Key by User

                    .map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {

                        // Keep track of last event name
                        private transient ValueState<String> lastEventName;
                        // Keep track of last event timestamp
                        private transient ValueState<Long> lastEventStart;

                        @Override
                        public void open(Configuration config) throws Exception {

                            // Setup state Stores
                            ValueStateDescriptor<String> nameDescriptor =
                                    new ValueStateDescriptor<String>("last-action-name", // state name
                                            TypeInformation.of(new TypeHint<String>() {}));

                            lastEventName = getRuntimeContext().getState(nameDescriptor);

                            ValueStateDescriptor<Long> startDescriptor =
                                    new ValueStateDescriptor<Long>("last-action-start", // state name
                                            TypeInformation.of(new TypeHint<Long>() {}));

                            lastEventStart = getRuntimeContext().getState(startDescriptor);
                        }

                        @Override
                        public Tuple3<String, String, Long> map(Tuple3<String, String, Long> user) throws Exception {

                            // Default to publish
                            String publishAction = "None";
                            Long publishDuration = 0L;

                            // Check if it's not the first event of the session
                            if (lastEventName.value() != null) {
                                // If login event, duration not applicable
                                if (! user.f1.equals("Login")) {
                                    // Set the last event name
                                    publishAction = lastEventName.value();
                                    // Last event duration = difference in timestamps
                                    publishDuration = user.f2 - lastEventStart.value();
                                }
                            }

                            // If logout event, unset the state trackers
                            if (user.f1.equals("Logout")) {
                                lastEventName.clear();
                                lastEventStart.clear();
                            }
                            // Update the state with current event
                            else {
                                lastEventName.update(user.f1);
                                lastEventStart.update(user.f2);
                            }

                            // Publish durations
                            return new Tuple3<String, String, Long>(user.f0, publishAction, publishDuration);
                        }
                    })
                    ;

            // Pretty Print
            userActionDuration.map(new MapFunction<Tuple3<String, String, Long>, Object>() {
                @Override
                public Object map(Tuple3<String, String, Long> summary) throws Exception {
                    System.out.println(ANSI_BLUE + "Durations : " +
                            " User : " + summary.f0 +
                            " Action : " + summary.f1 +
                            " Duration : " + summary.f2);
                    return null;
                }
            });

            // Start the kafka stream generator on a separate thread
            Utils.printHeader("String Kafka Data Generator...");
            Thread kafkaThread = new Thread(new KafkaEventGenerator());
            kafkaThread.start();

            env.execute("Flink Browser Use Case");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
