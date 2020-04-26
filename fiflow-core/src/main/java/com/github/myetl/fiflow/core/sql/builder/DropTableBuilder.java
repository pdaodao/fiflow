package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;

/**
 * drop table xx
 */
public class DropTableBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(DROP\\s+TABLE\\s+.*)";

    public DropTableBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "drop table xx; drop table xx";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        String sql = cmd.args[0];
        sessionContext.tEnv.sqlUpdate(sql);
        result.addMsg(sql + " ok");
        return result;
    }
}
