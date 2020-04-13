package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;

/**
 * drop view xx
 */
public class DropViewBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "DROP\\s+VIEW\\s+(.*)";

    public DropViewBuilder() {
        super(pattern);
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }


    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, FiflowSqlSession session) {
        return null;
    }
}
