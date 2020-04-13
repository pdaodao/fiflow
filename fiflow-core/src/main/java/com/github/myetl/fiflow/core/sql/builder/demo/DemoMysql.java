package com.github.myetl.fiflow.core.sql.builder.demo;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;

public class DemoMysql extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "demo\\s+mysql\\s?";

    public DemoMysql() {
        super(pattern);
    }

    @Override
    public String help() {
        return "demo mysql; jdbc simple demo";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Show;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, FiflowSqlSession session) {
        result.table().addHeads("jdbc simple demo");
        try {
            result.table().addRow(readText("demo-mysql.txt"));
        } catch (Exception e) {
            result.addMsg(e.getMessage());
        }
        return result;
    }
}
