package com.github.lessonone.fiflow.core.sql.builder.frame;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * set a = 1
 */
public class SetBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "SET(\\s+(\\S+)\\s*=(.*))?";

    public SetBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "se k = v; set configuration";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Set;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        if (cmd.args.length != 3) {
            result.addMsg("illegal set " + cmd.args[0]);
        } else {
            String k = cmd.args[1];
            String v = cmd.args[2];

            result.addMsg("set configuration " + k + "=" + v);
            session.tEnv.getConfig().getConfiguration().setString(k, v);
        }
        return result;
    }
}
