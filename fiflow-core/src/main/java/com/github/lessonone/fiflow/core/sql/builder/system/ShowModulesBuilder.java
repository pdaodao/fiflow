package com.github.lessonone.fiflow.core.sql.builder.system;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

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
    public BuildLevel buildLevel() {
        return BuildLevel.Show;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        result.table().addHeads("module name");
        for (String t : session.tEnv.listModules()) {
            result.table().addRow(t);
        }
        return result;
    }
}
