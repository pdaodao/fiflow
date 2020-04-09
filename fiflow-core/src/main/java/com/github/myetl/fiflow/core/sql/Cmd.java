package com.github.myetl.fiflow.core.sql;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Arrays;

public class Cmd {
    public CmdType cmdType;
    public String[] args;

    public Cmd(CmdType cmdType, String[] args) {
        this.cmdType = cmdType;
        this.args = args;
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
}
