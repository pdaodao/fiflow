package com.github.lessonone.fiflow.core.sql.builder.system;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * (QUIT|EXIT)
 * 退出
 */
public class QuitBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "(QUIT|EXIT)";

    public QuitBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "quit; close session ";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Error;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext context) {
        result.addMsg("close session and env");
        context.session.close();
        return result;
    }
}
