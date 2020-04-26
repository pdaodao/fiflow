package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;

/**
 * insert into t1(f1,f2,...) select f1, f2, ... from t2 where ...
 * 数据插入语句
 */
public class InsertIntoBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(INSERT\\s+INTO.*)";

    public InsertIntoBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "insert into; write data and trigger job submit";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Insert;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        final String sql = cmd.args[0];
        insert(sql, sessionContext);
        result.addMsg("prepare insert into " + SqlSplitUtil.getInsertIntoTableName(sql));
        return result;
    }

    public static  void insert(String sql, SqlSessionContext sessionContext){
        sessionContext.tEnv.sqlUpdate(sql);
    }
}
