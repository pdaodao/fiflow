package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;

/**
 * insert overwrite
 */
public class InsertOverwriteBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(INSERT\\s+OVERWRITE.*)";

    public InsertOverwriteBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "insert overwrite; insert overwrite ...";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Insert;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        final String sql = cmd.args[0];
        InsertIntoBuilder.insert(sql, sessionContext);
        result.addMsg("prepare insert overwrite " + SqlSplitUtil.getInsertIntoTableName(sql));
        return result;
    }
}
