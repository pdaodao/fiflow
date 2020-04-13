package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * show tables
 */
public class ShowTablesBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "SHOW\\s+TABLES";

    public ShowTablesBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "show tables; list tables";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Show;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, FiflowSqlSession session) {
        result.table().addHeads("tables_in " + session.tEnv.getCurrentCatalog() + "." + session.tEnv.getCurrentDatabase());

        for (String t : session.tEnv.listTables()) {
            result.table().addRow(t);
        }
        return result;
    }
}
