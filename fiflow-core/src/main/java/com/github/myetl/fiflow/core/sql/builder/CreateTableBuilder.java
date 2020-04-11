package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
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
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        String sql = cmd.args[0];
        FlinkBuildInfo sqlBuildResult = new FlinkBuildInfo(BuildLevel.Create);
        session.tEnv.sqlUpdate(sql);
        sqlBuildResult.addMsg("create table " + SqlSplitUtil.getCreateTableName(sql) + " ok");
        return sqlBuildResult;
    }
}
