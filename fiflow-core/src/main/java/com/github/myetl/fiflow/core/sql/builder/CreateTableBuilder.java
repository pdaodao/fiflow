package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;

/**
 * create table t1( ) with ( )
 */
public class CreateTableBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(CREATE\\s+TABLE\\s+.*)";

    public CreateTableBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "create table; table schema and connect info";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        String sql = cmd.args[0];
        sessionContext.tEnv.sqlUpdate(sql);
        result.addMsg("create table " + SqlSplitUtil.getCreateTableName(sql) + " ok");
        return result;
    }
}
