package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * use catalog xx
 */
public class UseCatalogBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "USE\\s+CATALOG\\s+(.*)";

    public UseCatalogBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "use catalog xx; The name of the catalog to set as the current default catalog";
    }

    @Override
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        final String catalog = cmd.args[0];
        FlinkBuildInfo result = new FlinkBuildInfo(BuildLevel.Set);
        result.addMsg("use catalog " + catalog);
        session.tEnv.useCatalog(catalog);
        return result;
    }
}
