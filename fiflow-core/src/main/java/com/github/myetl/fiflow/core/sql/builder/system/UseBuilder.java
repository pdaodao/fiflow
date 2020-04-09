package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * use xx
 */
public class UseBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "USE\\s+(?!CATALOG)(.*)";

    public UseBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "use xx; use database ";
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Set);
        String database = cmd.args[0];

        boolean has = false;
        for (String t : session.tEnv.listDatabases()) {
            if (database.equalsIgnoreCase(t)) {
                has = true;
            }
        }

        if (has == false)
            throw new IllegalArgumentException("database not exist " + database);

        result.addMsg("use database " + database);

        return result;
    }
}
