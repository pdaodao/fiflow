package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Arrays;

public class Cmd implements Serializable {
    public CmdType cmdType;
    public String[] args;
    public Cmd(CmdType cmdType, String[] args) {
        this.cmdType = cmdType;
        this.args = args;
    }

    public Cmd() {
    }

    public BuildLevel level(){
        return cmdType.cmdBuilder.buildLevel();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Cmd cmd = (Cmd) o;

        return new EqualsBuilder()
                .append(cmdType, cmd.cmdType)
                .append(args, cmd.args)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(cmdType)
                .append(args)
                .toHashCode();
    }

    @Override
    public String toString() {
        return cmdType + "(" + Arrays.toString(args) + ")";
    }

    public CmdType getCmdType() {
        return cmdType;
    }

    public Cmd setCmdType(CmdType cmdType) {
        this.cmdType = cmdType;
        return this;
    }

    public String[] getArgs() {
        return args;
    }

    public Cmd setArgs(String[] args) {
        this.args = args;
        return this;
    }

    public void preBuild(BuildContext buildContext, BuildContext previousContext) {
        cmdType.cmdBuilder.preBuild(this, buildContext, previousContext);
    }

    public FlinkBuildInfo build(FiflowSqlSession session) {
        return cmdType.cmdBuilder.build(this, session);
    }
}
