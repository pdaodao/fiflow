package com.github.lessonone.fiflow.common.learn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * The following program is a complete, working example of streaming window word count application,
 * that counts the words coming from a web socket in 5 second windows.
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataInputStream = env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for(String t : value.split(" ")){
                            out.collect(new Tuple2<String, Integer>(t, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataInputStream.print();

        env.execute("WindowWordCount");
    }

    /**
     * To run the example program, start the input stream with netcat first from a terminal:
     * nc -lk 9999
     */
}
