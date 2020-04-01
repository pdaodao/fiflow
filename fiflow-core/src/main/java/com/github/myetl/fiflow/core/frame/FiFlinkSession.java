package com.github.myetl.fiflow.core.frame;

import com.github.myetl.fiflow.core.flink.FlinkClusterInfo;
import com.github.myetl.fiflow.core.sql.SqlBuildContext;
import com.github.myetl.fiflow.core.sql.SqlCommander;
import com.github.myetl.fiflow.core.sql.SqlExecutor;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * 表示一个会话 在一个会话中有状态保持功能
 * 相当于一个客户端
 */
public class FiFlinkSession {
    private final String id;
    private final SessionConfig sessionConfig;
    public StreamExecutionEnvironment env;
    public EnvironmentSettings settings;
    public TableEnvironment tEnv;

    public FlinkClusterInfo flinkClusterInfo;
    private List<SqlCommander> cmds = new ArrayList<>();

    private int step = 0;

    public FiFlinkSession(String id, SessionConfig sessionConfig) {
        this.id = id;
        this.sessionConfig = sessionConfig;
        init();
    }

    public String getId() {
        return id;
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

        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
                System.out.println("onJobSubmitted.."+jobClient.getJobID().toString());
            }

            @Override
            public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
                System.out.println("onJobExecuted.."+jobExecutionResult.getJobID().toString()+":"+jobExecutionResult.getClass().getName());
            }
        });
    }

    /**
     * 执行sql 包括 ddl dml 以;分隔的多条 sql
     * @param sqls
     */
    public SqlBuildContext sql(String sqls) {
       List<String> lines = SqlSplitUtil.split(sqls);
       FlowContext flowContext = new FlowContext();

       boolean needExecute = false;
       for(String sql : lines){
           SqlExecutor sqlExecutor = new SqlExecutor(sql, this);
           SqlBuildContext sqlBuildContext = sqlExecutor.run(flowContext);
           if(sqlBuildContext.needExecute) needExecute = true;
       }
       SqlBuildContext sqlBuildContext = new SqlBuildContext(needExecute);
       sqlBuildContext.setFlowContext(flowContext);
       return sqlBuildContext;
    }

    public String getName(){
        return id+":"+step++;
    }

}
