package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

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
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Error);
        result.addMsg("close session and env");
        session.close();
        return result;
    }
}
