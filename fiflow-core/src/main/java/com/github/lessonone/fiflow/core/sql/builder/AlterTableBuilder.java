package com.github.lessonone.fiflow.core.sql.builder;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;

/**
 * alter table
 */
public class AlterTableBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(ALTER\\s+TABLE\\s+.*)";

    public AlterTableBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "alter table xx ...; alter table xx ... ";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        String sql = cmd.args[0];
        session.tEnv.sqlUpdate(sql);
        result.addMsg(sql);
        return result;
    }
}
