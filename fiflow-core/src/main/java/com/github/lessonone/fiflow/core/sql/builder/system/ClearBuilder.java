package com.github.lessonone.fiflow.core.sql.builder.system;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * clear
 */
public class ClearBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "CLEAR";

    public ClearBuilder() {
        super(pattern);
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Set;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        return null;
    }
}
