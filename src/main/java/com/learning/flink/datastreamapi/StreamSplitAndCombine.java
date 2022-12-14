package com.learning.flink.datastreamapi;

import com.learning.flink.common.MapCountPrinter;
import com.learning.flink.common.Utils;
import com.learning.flink.datasource.CsvFileGeneration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class StreamSplitAndCombine {

    public static void main(String[] args) {

        try {
            /****************************************************************
             *              Setup Flink Execution Environment
             ***************************************************************/

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            /****************************************************************
             *              Read Csv File into a DataStream
             ***************************************************************/

            // Specify input data directory
            String dataDir = "data/raw_audit_trail";

            // Specify the input format of the stream
            TextInputFormat auditFormat = new TextInputFormat(new Path(dataDir));

            // Create a DataStream based on the directory
            DataStream<String> auditTrailStr = env.readFile(auditFormat,
                                                dataDir, // Directory to monitor for new files
                                                FileProcessingMode.PROCESS_CONTINUOUSLY,
                                                1000);

            /****************************************************************
             *     Split the Stream into two Streams based on the Entity
             ***************************************************************/

            // Create a Separate Trail for the Sales Rep operation
            final OutputTag<Tuple2<String, Integer>> salesRepTag =
                    new OutputTag<Tuple2<String, Integer>>("sales-rep"){};

            // Convert each record into an Object
            SingleOutputStreamOperator<AuditTrail> customerTrail = auditTrailStr.process(new ProcessFunction<String, AuditTrail>() {
                @Override
                public void processElement(String auditStr, Context context, Collector<AuditTrail> collector) throws Exception {
                    System.out.println("--- Received Record : " + auditStr);

                    // Convert String to AuditTrail Object
                    AuditTrail auditTrail = new AuditTrail(auditStr);

                    // Create output tuple with User and count
                    Tuple2<String, Integer> entityCount = new Tuple2<String, Integer>(auditTrail.getUser(), 1);

                    if (auditTrail.getEntity().equals("Customer")) {
                        // Collect main output for Customer as AuditTrail
                        collector.collect(auditTrail);
                    } else {
                        // Collect side output for Sales Rep
                        context.output(salesRepTag, entityCount);
                    }
                }
            });

            // Convert side output into a DataStream
            DataStream<Tuple2<String, Integer>> salesRepTrail = customerTrail.getSideOutput(salesRepTag);

            // Print Customer Record summaries
            MapCountPrinter.printCount(customerTrail.map(i -> (Object) i), "Customer Records in Trail : Last 5 secs");

            // Print Sales Rep Record summaries
            MapCountPrinter.printCount(salesRepTrail.map(i -> (Object) i), "Sales Rep Records in Trail : Last 5 secs");

            /***************************************************
             *          Combine two Streams into one
             **************************************************/

            ConnectedStreams<AuditTrail, Tuple2<String, Integer>> mergedTrail = customerTrail.connect(salesRepTrail);

            DataStream<Tuple3<String, String, Integer>> processedTrail = mergedTrail.map(
                    new CoMapFunction<AuditTrail, // Stream 1
                            Tuple2<String, Integer>, // Stream 2
                            Tuple3<String, String, Integer> // Output
                            >() {
                @Override
                public Tuple3<String, String, Integer> // Process Stream 1
                    map1(AuditTrail auditTrail) throws Exception {
                    return new Tuple3<String, String, Integer>("Stream-1", auditTrail.getUser(), 1);
                }

                @Override
                public Tuple3<String, String, Integer> // Process Stream 2
                    map2(Tuple2<String, Integer> srTrail) throws Exception {
                    return new Tuple3<String, String, Integer>("Stream-2", srTrail.f0, 1);
                }
            });

            // Print and combine the DataStreams
            processedTrail.map(new MapFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, Integer>>() {

                @Override
                public Tuple3<String, Integer, Integer> map(Tuple3<String, String, Integer> user) throws Exception {
                    System.out.println("--- Merged Record for User: " + user);
                    return null;
                }
            });

            /******************************************************************
             *          Setup data source and execute the Flink Pipeline
             *****************************************************************/

            // Start the File Stream generator on a separate Thread
            Utils.printHeader("Starting Csv File Generator...");
            Thread genThread = new Thread(new CsvFileGeneration());
            genThread.start();

            // execute the streaming pipeline
            env.execute("Flink Streaming Split and Combine Stream");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
