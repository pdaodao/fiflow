package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

/**
 * show catalogs
 */
public class ShowCatalogsBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "SHOW\\s+CATALOGS";

    public ShowCatalogsBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "show catalogs; gets the names of all catalogs registered in this environment";
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Show);
        result.table().addHeads("catalogs");
        for (String t : session.tEnv.listCatalogs()) {
            result.table().addRow(t);
        }
        return result;
    }
}
