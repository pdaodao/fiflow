package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;

/**
 * select  数据查询
 */
public class SelectBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(SELECT.*)";

    public SelectBuilder() {
        super(pattern);
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        return null;
    }

    @Override
    public String help() {
        return "select ; query data and trigger job submit ";
    }
}
