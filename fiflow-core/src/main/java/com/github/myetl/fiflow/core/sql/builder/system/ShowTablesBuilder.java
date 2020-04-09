package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
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
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Show);
        result.table().addHeads("tables_in_" + session.tEnv.getCurrentCatalog() + "." + session.tEnv.getCurrentDatabase());

        for (String t : session.tEnv.listTables()) {
            result.table().addRow(t);
        }
        return result;
    }
}
