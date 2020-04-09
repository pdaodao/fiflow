package com.github.myetl.fiflow.core.sql.builder.frame;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

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
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        return null;
    }
}
