package com.github.lessonone.fiflow.io.elasticsearch7.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DemoBase {

    public static StreamExecutionEnvironment env;
    public static StreamTableEnvironment tEnv;


    static {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        tEnv = StreamTableEnvironment
                .create(env, EnvironmentSettings
                        .newInstance()
                        .inStreamingMode()
                        .useBlinkPlanner()
                        .build());
    }

}
