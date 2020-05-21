package com.github.lessonone.fiflow.core.sql;

import java.util.List;

public interface CmdList {

    void addCmd(Cmd cmd);

    void addCmdAll(List<Cmd> cmds);

    List<Cmd> getCmdList();

    void setCmdList(List<Cmd> cmdList);
}
