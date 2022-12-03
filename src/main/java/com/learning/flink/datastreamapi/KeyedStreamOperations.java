package com.learning.flink.datastreamapi;

import com.learning.flink.common.Utils;
import com.learning.flink.datasource.CsvFileGeneration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class KeyedStreamOperations {

    public static void main(String[] args) {
        try {

            /**************************************************
             *          Setup Flink Environment
             *************************************************/

            // Setup Flink Execution Environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Unless set explicitly, defaults to the number of cores in the machine
            System.out.println("\nTotal Parallel Task Slots : " + env.getParallelism());

            /**************************************************
             *          Read Csv File Stream into a DataStream
             *************************************************/

            // Define the data directory to monitor new files
            String dataDir = "data/raw_audit_trail";

            // Define the text input format based on the directory
            TextInputFormat auditFormat = new TextInputFormat(new Path(dataDir));

            // Create a DataStream based on the directory
            DataStream<String> auditTrailStr = env.readFile(auditFormat,
                                                    dataDir, // Directory to monitor
                                                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                                                    1000);

            /**************************************************
             *          Key By User, find Running count by user
             *************************************************/

            DataStream<Tuple2<String, Integer>> userCounts = auditTrailStr
                    .map(new MapFunction<String, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> map(String auditStr) throws Exception {
                            AuditTrail at = new AuditTrail(auditStr);
                            return new Tuple2<String, Integer>(at.getUser(), at.getDuration());
                        }
                    })
                    .keyBy(0) // By user name
                    .reduce((x,y) -> new Tuple2<String, Integer>(x.f0, x.f1 + y.f1));

            userCounts.print();

            /**************************************************
             *          Setup data source and execute the Flink Pipeline
             *************************************************/

            // Start the File Stream generator on a separate Thread
            Utils.printHeader("Starting Csv File Generator...");
            Thread genThread = new Thread(new CsvFileGeneration());
            genThread.start();

            // execute the streaming pipeline
            env.execute("Flink Streaming Keyed Stream");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



















