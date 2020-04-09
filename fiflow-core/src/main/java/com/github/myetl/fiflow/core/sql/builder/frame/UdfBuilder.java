package com.github.myetl.fiflow.core.sql.builder.frame;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

public class UdfBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "-*\\s?udf\\s+(.*)";

    public UdfBuilder() {
        super(pattern);
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        return null;
    }
}
