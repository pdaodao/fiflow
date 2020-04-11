package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.util.FlinkUtils;
import org.apache.flink.table.api.Table;

/**
 * select  数据查询
 */
public class SelectBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(SELECT.*)";

    public SelectBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "select ; query data and trigger job submit <span style='color:red;'>busy doing</span>";
    }

    @Override
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        FlinkBuildInfo result = new FlinkBuildInfo(BuildLevel.Select);
        final String sql = cmd.args[0];
        Table table = session.tEnv.sqlQuery(sql);

        try{
            FlinkUtils.collect(table);
        }catch (Exception e){
            e.printStackTrace();
        }

//        SocketClientSink
        // CollectSink


        return result;
    }
}
