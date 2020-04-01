package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.flink.FlinkClusterInfo;
import com.github.myetl.fiflow.core.frame.SessionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 核心 入口
 */
public abstract class FiflowSession {

    public final String id;
    public final SessionConfig sessionConfig;

    public StreamExecutionEnvironment env;
    public EnvironmentSettings settings;
    public TableEnvironment tEnv;

    public FlinkClusterInfo flinkClusterInfo;

    public FiflowSession(String id, SessionConfig sessionConfig) {
        this.id = id;
        this.sessionConfig = sessionConfig;

        init();
    }

    private void init() {
        if(env != null) return;

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(sessionConfig.parallelism);

        EnvironmentSettings.Builder settingBuilder = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner();
        if(sessionConfig.streamingMode){
            settingBuilder.inStreamingMode();
        }else {
            settingBuilder.inBatchMode();
        }
        settings = settingBuilder.build();

        tEnv = StreamTableEnvironment.create(env, settings);
    }

    /**
     * 执行sql
     * @param sqlText  多行以;分隔的sql语句
     */
    abstract void sql(String sqlText);

    /**
     * 关闭该 session
     */
    abstract void close();
}
