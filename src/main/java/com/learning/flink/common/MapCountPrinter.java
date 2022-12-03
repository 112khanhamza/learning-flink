package com.learning.flink.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MapCountPrinter {

    public static void printCount(DataStream<Object> dsObj, String msg) {
        dsObj
                // Generate a counter record for each object
                .map(i -> new Tuple2<String, Integer>(msg, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))

                // Window by time = 5 sec
                .timeWindowAll(Time.seconds(5))

                // Sum the number of records for each 5 sec interval
                .reduce((x,y) -> (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)))

                // Print the summary
                .map(new MapFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple2<String, Integer> recordCount) throws Exception {
                        Utils.printHeader(recordCount.f0 + " : " + recordCount.f1);
                        return recordCount.f1;
                    }
                });
    }
}
