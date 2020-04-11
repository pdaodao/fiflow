package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * clear
 */
public class ClearBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "CLEAR";

    public ClearBuilder() {
        super(pattern);
    }


    @Override
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        return null;
    }
}
