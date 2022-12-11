package com.learning.flink.state;

import com.learning.flink.common.Utils;
import com.learning.flink.datasource.CsvFileGeneration;
import com.learning.flink.datastreamapi.AuditTrail;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class StatefulOperations {
    public static void main(String[] args) {
        try {

            /********************************************************
             *          Setup Stream Execution Env
             ******************************************************/

            // Setup Flink Environment
//            Configuration conf = new Configuration();
//            final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            /********************************************************
             *          Read String DataStream from DataSource
             ******************************************************/

            String dataDir = "data/raw_audit_trail";
            TextInputFormat textInputFormat = new TextInputFormat(new Path(dataDir));

            DataStream<String> auditTrailStr = env.readFile(textInputFormat,
                    dataDir,
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    1000);

            /********************************************************
             *          Use Simple Stateful Operations
             ******************************************************/

            // Convert each record into an Object
            DataStream<Tuple3<String, String, Long>> auditTrailState = auditTrailStr.map(new MapFunction<String, Tuple3<String, String, Long>>() {
                @Override
                public Tuple3<String, String, Long> map(String s) throws Exception {
                    System.out.println("--- Received Record : " + s);
                    AuditTrail auditTrail = new AuditTrail(s);
                    return new Tuple3<String, String, Long>(
                            auditTrail.getUser(),
                            auditTrail.getOperation(),
                            auditTrail.getTimestamp()
                    );
                }
            });

            // Measure the time interval between DELETE operations by the same user
            DataStream<Tuple2<String, Long>> deleteIntervals =
                    auditTrailState
                    .keyBy(0)
                    .map(new RichMapFunction<Tuple3<String, String, Long>, Tuple2<String, Long>>() {

                        private transient ValueState<Long> lastDelete;

                        @Override
                        public void open(Configuration config) throws Exception {
                            ValueStateDescriptor<Long> descriptor =
                                    new ValueStateDescriptor<Long>("last-delete", // the state name
                                    TypeInformation.of(new TypeHint<Long>() {}));

                            lastDelete = getRuntimeContext().getState(descriptor);
                        }

                        @Override
                        public Tuple2<String, Long> map(Tuple3<String, String, Long> auditTrail) throws Exception {
                            Tuple2<String, Long> retTuple = new Tuple2<String, Long>("No Alerts", 0L);

                            // If two deletes were done by the same user within 10 seconds
                            if (auditTrail.f1.equals("Delete")) {
                                if (lastDelete.value() != null) {
                                    long timeDiff = auditTrail.f2 - lastDelete.value();
                                    if (timeDiff < 10000L) {
                                        retTuple = new Tuple2<String, Long>(auditTrail.f0, timeDiff);
                                    }
                                }
                                lastDelete.update(auditTrail.f2);
                            }
                            // If no specific alert record was returned
                            return retTuple;
                        }
                    })
                    .filter(new FilterFunction<Tuple2<String, Long>>() {
                        @Override
                        public boolean filter(Tuple2<String, Long> alert) throws Exception {
                            if (alert.f0.equals("No-Alerts")) {
                                return false;
                            } else {
                                System.out.println("\n!! DELETE Alert Received : User "
                                + alert.f0 + " executed 2 deleted within "
                                + alert.f1 + " ms" + "\n");
                                return true;
                            }
                        }
                    })
                    ;

            /************************************************************************
             *                SETUP DATASOURCE AND EXECUTE THE FLINK PIPELINE
             ***********************************************************************/

            // Start the Csv Generator on a separate thread
            Utils.printHeader("String Csv File Generation");
            Thread genThread = new Thread(new CsvFileGeneration());
            genThread.start();

            // Execute the streaming pipeline
            env.execute("Stateful Operations Streaming Audit Trail");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
