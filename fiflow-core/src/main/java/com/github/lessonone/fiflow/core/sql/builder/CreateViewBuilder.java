package com.github.lessonone.fiflow.core.sql.builder;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import org.apache.flink.table.api.Table;

/**
 * create view
 */
public class CreateViewBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "CREATE\\s+VIEW\\s+(\\S+)\\s+AS\\s+(.*)";

    public CreateViewBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "create view; create view  xx as select ...";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Create;
    }


    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext sessionContext) {
        String name = cmd.args[0];
        String sql = cmd.args[1];
        Table table = sessionContext.tEnv.sqlQuery(sql);
        sessionContext.tEnv.createTemporaryView(name, table);
        result.addMsg("create view " + name + " ok ");
        return result;
    }
}
