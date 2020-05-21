package com.github.lessonone.fiflow.core.sql.builder.system;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

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
    public BuildLevel buildLevel() {
        return BuildLevel.Show;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        result.table().addHeads("database name");
        for (String t : session.tEnv.listDatabases()) {
            result.table().addRow(t);
        }
        return result;
    }
}
