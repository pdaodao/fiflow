package com.github.lessonone.fiflow.core.sql.builder.system;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

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
    public BuildLevel buildLevel() {
        return BuildLevel.Set;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        final String catalog = cmd.args[0];
        result.addMsg("use catalog " + catalog);
        session.tEnv.useCatalog(catalog);
        return result;
    }
}
