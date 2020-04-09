package com.github.myetl.fiflow.core.sql.builder.frame;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

/**
 * -p n
 * 设置任务的并发度
 */
public class ParallelismBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "-*\\s?-p\\s+(\\d)";

    public ParallelismBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "-p n; set job parallelism";
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        String p = cmd.args[0];

        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Set);
        result.addMsg("set parallelism " + p);
        Integer pp = Integer.parseInt(p);

        session.tEnv.getConfig().getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, pp);

        return result;
    }
}
