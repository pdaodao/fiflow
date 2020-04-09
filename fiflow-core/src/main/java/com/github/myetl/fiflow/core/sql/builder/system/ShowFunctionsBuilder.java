package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * show functions
 */
public class ShowFunctionsBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "SHOW\\s+FUNCTIONS";

    public ShowFunctionsBuilder() {
        super(pattern);
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        return null;
    }
}
