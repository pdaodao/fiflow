package com.github.lessonone.fiflow.core.sql.builder;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.util.FlinkUtils;
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
    public BuildLevel buildLevel() {
        return BuildLevel.Select;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        final String sql = cmd.args[0];
        Table table = sessionContext.tEnv.sqlQuery(sql);

        try {
            FlinkUtils.collect(table);
        } catch (Exception e) {
            e.printStackTrace();
        }

//        SocketClientSink
        // CollectSink


        return result;
    }
}
