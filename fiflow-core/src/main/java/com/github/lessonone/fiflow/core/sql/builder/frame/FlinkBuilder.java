package com.github.lessonone.fiflow.core.sql.builder.frame;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * flink xx 选择flink集群
 */
public class FlinkBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "-*\\s?flink\\s+(.*)";

    public FlinkBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "flink xx; select flink cluster";
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
