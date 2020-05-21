package com.github.lessonone.fiflow.core.sql;

import java.util.ArrayList;
import java.util.List;

public class CmdListImpl implements CmdList {
    private List<Cmd> cmdList = new ArrayList<>();

    @Override
    public void addCmd(Cmd cmd) {
        if (cmd != null)
            cmdList.add(cmd);
    }

    @Override
    public void addCmdAll(List<Cmd> cmds) {
        if (cmds != null)
            cmdList.addAll(cmds);
    }

    @Override
    public List<Cmd> getCmdList() {
        return cmdList;
    }

    @Override
    public void setCmdList(List<Cmd> cmdList) {
        this.cmdList = cmdList;
    }
}
