package com.github.lessonone.fiflow.core.sql.builder.demo;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;

public abstract class DemoBase extends CmdBaseBuilder implements CmdBuilder {
    private String name;

    public DemoBase(String name) {
        super("demo\\s+" + name + "\\s?");
        this.name = name;
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Show;
    }

    protected abstract String demoFileName();

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        result.table().addHeads("demo " + name);
        try {
            result.table().addRow(readText(demoFileName()));
        } catch (Exception e) {
            result.addMsg(e.getMessage());
        }
        return result;
    }
}
