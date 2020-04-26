package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;

/**
 * drop database xx
 */
public class DropDatabaseBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(DROP\\s+DATABASE\\s+.*)";

    public DropDatabaseBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "drop database xx; drop database xx";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        String sql = cmd.args[0];
        sessionContext.tEnv.sqlUpdate(sql);
        result.addMsg(sql);

        return result;
    }
}
