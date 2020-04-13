package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;

/**
 * drop database xx
 */
public class DropDatabaseBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(DROP\\s+DATABASE\\s+.*)";

    public DropDatabaseBuilder() {
        super(pattern);
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Insert;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, FiflowSqlSession session) {
        return null;
    }
}
