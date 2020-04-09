package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;
import org.apache.flink.table.api.Table;

/**
 * explain xx
 */
public class ExplainBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "EXPLAIN\\s+(.*)";

    public ExplainBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "explain; the AST and SQL queries and the execution plan to compute";
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        final String sql = cmd.args[0];
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Show);
        result.table().addHeads("AST and execution plan");

        Table table = session.tEnv.sqlQuery(sql);
        String msg = session.tEnv.explain(table);
        result.table().addRow(msg);
        return result;
    }
}
