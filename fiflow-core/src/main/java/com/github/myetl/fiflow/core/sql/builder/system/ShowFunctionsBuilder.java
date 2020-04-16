package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * show functions
 */
public class ShowFunctionsBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "SHOW\\s+FUNCTIONS";

    public ShowFunctionsBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "show functions; gets the names of all functions in this environment";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Show;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        result.table().addHeads("functions", "is user defined ");
        for (String t : session.tEnv.listFunctions()) {
            result.table().addRow(t, "false");
        }
        for (String t : session.tEnv.listUserDefinedFunctions()) {
            result.table().addRow(t, "true");
        }
        return result;
    }
}
