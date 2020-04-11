package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * show modules
 */
public class ShowModulesBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "SHOW\\s+MODULES";

    public ShowModulesBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "show modules; gets an array of names of all modules in this environment in the loaded order";
    }

    @Override
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        FlinkBuildInfo result = new FlinkBuildInfo(BuildLevel.Show);
        result.table().addHeads("module name");
        for (String t : session.tEnv.listModules()) {
            result.table().addRow(t);
        }
        return result;
    }
}
