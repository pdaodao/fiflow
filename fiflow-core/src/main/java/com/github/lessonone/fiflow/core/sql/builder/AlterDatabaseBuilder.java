package com.github.lessonone.fiflow.core.sql.builder;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;

/**
 * alter database
 */
public class AlterDatabaseBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(ALTER\\s+DATABASE\\s+.*)";

    public AlterDatabaseBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "alter database xx; alter database xxx";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        String sql = cmd.args[0];
        session.tEnv.sqlUpdate(sql);
        return result;
    }
}
