package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;

/**
 * create view
 */
public class CreateViewBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(.*)";

    public CreateViewBuilder() {
        super(pattern);
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }


    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        return null;
    }
}
