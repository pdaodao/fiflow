package com.github.myetl.flow.core;

import com.github.myetl.flow.core.exception.FlowException;
import com.github.myetl.flow.core.parser.SqlParser;
import com.github.myetl.flow.core.parser.SqlTree;
import com.github.myetl.flow.core.runtime.SqlTreeCompiler;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flow {
    private static final Logger logger = LoggerFactory.getLogger(Flow.class);

    final String sql;

    public Flow(String sql) {
        this.sql = sql;
    }

    public boolean check() throws FlowException {
        // todo
        return true;
    }

    /**
     * simple execute
     *
     * @throws Exception
     */
    public void execute() throws Exception {
        SqlTree sqlTree = SqlParser.parseSql(sql);
        if (sqlTree.isStream()) {
            logger.info("-- run sql flow as StreamExecutionEnvironment ");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

            SqlTreeCompiler.compile(sqlTree, tableEnv, env.getConfig());
            env.execute();
        } else {
            logger.info("-- run sql as Batch ExecutionEnvironment ");
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            TableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            SqlTreeCompiler.compile(sqlTree, tableEnv, env.getConfig());
            env.execute();
        }
    }
}
