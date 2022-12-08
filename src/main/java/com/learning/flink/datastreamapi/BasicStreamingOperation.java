package com.learning.flink.datastreamapi;

import com.learning.flink.common.MapCountPrinter;
import com.learning.flink.common.Utils;
import com.learning.flink.datasource.CsvFileGeneration;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;

public class BasicStreamingOperation {

    public static void main(String[] args) {
        try {

            /************************************************************************
             *                SETUP FLINK STREAMING ENVIRONMENT
             ***********************************************************************/

            // Setup Flink Environment
            Configuration conf = new Configuration();
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Keeps the ordering of records. Else multiple threads can change sequence of printing.
            env.setParallelism(1);

            /************************************************************************
             *                READ CSV FILE TO STREAM DATA TO A DATASTREAM
             ***********************************************************************/

            // Define the data directory to monitor new files
            String dir = "data/raw_audit_trail";

            // Define the text input format based on the directory
            TextInputFormat auditFormat = new TextInputFormat(new Path(dir));

            // Create a DataStream based on that directory
            DataStream<String> auditTrailStr = env.readFile(auditFormat,
                                                    dir, // Directory to monitor
                                                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                                                    1000);

            // Convert each record in the stream to an Object
            DataStream<AuditTrail> auditTrailObj = auditTrailStr.map(new MapFunction<String, AuditTrail>() {
                @Override
                public AuditTrail map(String auditStr) throws Exception {
                    System.out.println("---Received Record : " + auditStr);
                    return new AuditTrail(auditStr);
                }
            });

            /************************************************************************
             *                PERFORM COMPUTATION AND WRITE TO AN OUTPUT SINK
             ***********************************************************************/

            // Print message for audit trail count
            MapCountPrinter.printCount(auditTrailObj.map(i -> (Object) i), "Audit Trail : Last 5 seconds");

            // Window by 5 seconds, count #of records and save to output
            DataStream<Tuple2<String, Integer>> recordCount = auditTrailObj.map(i ->
                    new Tuple2<String, Integer>(String.valueOf(System.currentTimeMillis()), 1))
                    .returns(Types.TUPLE(Types.STRING, Types.INT))
                    .timeWindowAll(Time.seconds(5))
                    .reduce((x,y) -> new Tuple2<String, Integer>(x.f0, x.f1 + y.f1));

            // Define output directory to store summary information
            String outDir = "data/five_sec_summary";

            // Clean existing files in the directory
            FileUtils.cleanDirectory(new File(outDir));

            // Set up a streaming file sink to the output directory
            final StreamingFileSink<Tuple2<String, Integer>> countSink = StreamingFileSink
                            .forRowFormat(new Path(outDir),
                            new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8"))
                            .build();

            // Add file sink as a sink to the data stream
            recordCount.addSink(countSink);

            /************************************************************************
             *                SETUP DATASOURCE AND EXECUTE THE FLINK PIPELINE
             ***********************************************************************/

            // Start the Csv Generator on a separate thread
            Utils.printHeader("String Csv File Generation");
            Thread genThread = new Thread(new CsvFileGeneration());
            genThread.start();

            // Execute the streaming pipeline
            env.execute("Flink Streaming Audit Trail");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
