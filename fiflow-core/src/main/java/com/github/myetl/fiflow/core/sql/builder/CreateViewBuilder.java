package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;

/**
 * create view
 */
public class CreateViewBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(.*)";

    public CreateViewBuilder() {
        super(pattern);
    }

    @Override
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        return null;
    }
}
