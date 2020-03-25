package com.github.myetl.fiflow.core.frame;

import com.github.myetl.fiflow.core.sql.SqlExecutor;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.List;

/**
 * 表示一个会话 在一个会话中有状态保持功能
 * 相当于一个客户端
 */
public class FlowSession {
    private final String id;
    private final SessionConfig sessionConfig;
    public StreamExecutionEnvironment env;
    public EnvironmentSettings settings;
    public TableEnvironment tEnv;

    public FlowSession(String id, SessionConfig sessionConfig) {
        this.id = id;
        this.sessionConfig = sessionConfig;
    }

    // 初始化 各种 env
    public synchronized void init() {
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
     * 执行sql 包括 ddl dml 以;分隔的多条 sql
     * @param sqls
     */
    public void sql(String sqls) {
       List<String> lines = SqlSplitUtil.split(sqls);
       FlowContext flowContext = new FlowContext();

       for(String sql : lines){
           SqlExecutor sqlExecutor = new SqlExecutor(sql, this);
           sqlExecutor.run(flowContext);
       }

    }

}
