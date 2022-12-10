package com.learning.flink.event;

import com.learning.flink.common.Utils;
import com.learning.flink.datasource.CsvFileGeneration;
import com.learning.flink.datastreamapi.AuditTrail;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Properties;

public class EventTimeOperations {
    public static void main(String[] args) {
        try {

            /**********************************************************
             *          SETUP STREAM EXECUTION ENVIRONMENT
             *********************************************************/

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            /**********************************************************
             *          Read CSV File
             *********************************************************/

            String dataDir = "data/raw_audit_trail";
            TextInputFormat textInputFormat = new TextInputFormat(new Path(dataDir));
            DataStream<String> auditTrailStr = env.readFile(textInputFormat,
                    dataDir,
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    1000);

            // Convert Stream of String to Object
            DataStream<AuditTrail> auditTrailObj = auditTrailStr.map(new MapFunction<String, AuditTrail>() {
                @Override
                public AuditTrail map(String s) throws Exception {
                    System.out.println("--- Received Record : " + s);
                    return new AuditTrail(s);
                }
            });

            /**********************************************************
             *          SETUP Event Time and WaterMarks
             *********************************************************/

            // Setting up a Watermark with 2 sec delay
            DataStream<AuditTrail> auditTrailET = auditTrailObj.assignTimestampsAndWatermarks(
                    new AssignerWithPunctuatedWatermarks<AuditTrail>() {

                        // Extract Watermark
                        transient long currentWaterMark = 0L;
                        int delay = 10000; // 10sec
                        int buffer = 2000; // 2sec

                @Nullable
                @Override
                public Watermark checkAndGetNextWatermark(AuditTrail auditTrail, long newTimestamp) {
                    long currentTime = System.currentTimeMillis();
                    if (currentWaterMark == 0L) {
                        currentWaterMark = currentTime;
                    }
                    // update watermark every 10 sec
                    else if (currentTime - currentWaterMark > delay) {
                        currentWaterMark = currentTime;
                    }
                    // return WaterMark adjusted to buffer
                    return new Watermark(currentWaterMark - buffer);
                }

                // Extract event timestamp value
                @Override
                public long extractTimestamp(AuditTrail auditTrail, long previousTimestamp) {
                    return auditTrail.getTimestamp();
                }
            });

            /**********************************************************
             *          Process a WaterMarked Stream
             *********************************************************/

            // Create a separate trail for late events
            final OutputTag<Tuple2<String,Integer>> lateAuditTrail
                    = new OutputTag<Tuple2<String,Integer>>("late-audit-trail"){};

            SingleOutputStreamOperator<Tuple2<String, Integer>> finalTrail = auditTrailET
                    // get the event timestamp and count
                    .map(i -> new Tuple2<String, Integer>(String.valueOf(i.getTimestamp()), 1))

                    // Specify the return type
                    .returns(Types.TUPLE(Types.STRING, Types.INT))

                    // Window by 1 sec
                    .timeWindowAll(Time.seconds(1))

                    // Handle late data
                    .sideOutputLateData(lateAuditTrail)

                    // Find total records every second
                    .reduce((x, y) ->
                            (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)))

                    // Pretty print
                    .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> map(Tuple2<String, Integer> minuteSummary) throws Exception {
                            String currentTime = (new Date()).toString();
                            String eventTime = (new Date(Long.valueOf(minuteSummary.f0))).toString();
                            System.out.println("Summary : "
                            + " Current Time : " + currentTime
                            + " Event Time : " + eventTime
                            + " Count : " + minuteSummary.f1);

                            return minuteSummary;
                        }
                    })
                    ;

            // Collect late events and process them later
            DataStream<Tuple2<String, Integer>> lateTrail =
                    finalTrail.getSideOutput(lateAuditTrail);

            /****************************************************************************
             *                  Send Processed Results to a Kafka Sink
             ****************************************************************************/

            // Setup properties for Kafka connection
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("bootstrap.servers", "localhost:9092");

            // Create a producer for kafka
            FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                    // Topic name
                    "flink.kafka.streaming.sink",

                    // Serialization for String data
                    (new KafkaSerializationSchema<String>() {
                        @Override
                        public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                            return (new ProducerRecord<byte[], byte[]>("flink.streaming.sink", s.getBytes()));
                        }
                    }),

                    kafkaProps,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            );

            // Publish to write to kafka
            finalTrail // Convert to String to write to Kafka
                    .map(new MapFunction<Tuple2<String, Integer>, String>() {
                        @Override
                        public String map(Tuple2<String, Integer> finalTrail) throws Exception {
                            return finalTrail.f0 + " = " + finalTrail.f1;
                        }
                    })
                    // Add Producer to Sink
                    .addSink(kafkaProducer);

            /************************************************************************
             *                SETUP DATASOURCE AND EXECUTE THE FLINK PIPELINE
             ***********************************************************************/

            // Start the Csv Generator on a separate thread
            Utils.printHeader("String Csv File Generation");
            Thread genThread = new Thread(new CsvFileGeneration());
            genThread.start();

            // Execute the streaming pipeline
            env.execute("Event Time Streaming Audit Trail");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
