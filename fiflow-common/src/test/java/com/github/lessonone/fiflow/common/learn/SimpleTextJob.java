package com.github.lessonone.fiflow.common.learn;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Iterator;

public class SimpleTextJob {

    public static void main(String[] args) throws Exception {
        final String rootPath = "/Users/pengda/flink/fiflow/";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> input =  env.readTextFile("student.csv");

        DataStream<Row> student = input.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] t = value.split(",");
                return Row.of(t[0], Integer.parseInt(t[1]));
            }
        });

        Iterator<Row> it = DataStreamUtils.collect(student);
        System.out.println("=========== it start =========== ");
        while(it.hasNext()){
            System.out.println(it.next().toString());
        }
        System.out.println("=========== it end =========== ");



//        student.writeAsText(rootPath+"haha.csv");

//        JobClient jobClient = env.executeAsync("simple text job");



    }


}
