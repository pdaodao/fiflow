package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * show databases
 */
public class ShowDatabasesBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "SHOW\\s+DATABASES";

    public ShowDatabasesBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "show databases; gets the names of all databases registered in the current catalog";
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Show);
        result.table().addHeads("database name");
        for (String t : session.tEnv.listDatabases()) {
            result.table().addRow(t);
        }
        return result;
    }
}
