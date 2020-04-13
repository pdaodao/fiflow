package com.github.myetl.fiflow.core.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * sql 转换为 flink 时的上下文
 */
public class BuildContext implements Serializable {
    // sql cmd list
    private List<Cmd> cmdList = new ArrayList<>();
    // is streaming mode
    private Boolean streamingMode = false;

    public void addCmd(Cmd cmd) {
        if (cmd != null)
            this.cmdList.add(cmd);
    }

    public void addCmdAll(List<Cmd> cmds) {
        if (cmds != null)
            this.cmdList.addAll(cmds);
    }

    public List<Cmd> getCmdList() {
        return cmdList;
    }

    public BuildContext setCmdList(List<Cmd> cmdList) {
        this.cmdList = cmdList;
        return this;
    }

    public Boolean getStreamingMode() {
        return streamingMode;
    }

    public BuildContext setStreamingMode(Boolean streamingMode) {
        this.streamingMode = streamingMode;
        return this;
    }
}
