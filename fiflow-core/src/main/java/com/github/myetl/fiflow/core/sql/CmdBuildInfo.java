package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.pojo.TableData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 单条 sql 解析执行后的结果
 */
public class CmdBuildInfo implements Serializable {
    // 构建级别
    private final BuildLevel level;
    // 任务id
    private String jobId;
    // 信息
    private List<String> msgs;
    // 表格数据
    private TableData table;
    // session id
    private String sessionId;

    public CmdBuildInfo(BuildLevel level) {
        this.level = level;
    }

    public static CmdBuildInfo of(BuildLevel level, TableData rowSet) {
        CmdBuildInfo r = new CmdBuildInfo(level);
        r.setTable(rowSet);
        return r;
    }

    public CmdBuildInfo addMsg(String msg) {
        if (StringUtils.isEmpty(msg)) return this;
        if (msgs == null) msgs = new ArrayList<>();
        msgs.add(msg);
        return this;
    }

    public CmdBuildInfo addAllMsg(List<String> msg) {
        if (msg == null) return this;
        if (msgs == null) msgs = new ArrayList<>();
        msgs.addAll(msg);
        return this;
    }

    public BuildLevel getLevel() {
        return level;
    }

    public TableData getTable() {
        return table;
    }

    public CmdBuildInfo setTable(TableData table) {
        if (table == null) return this;
        this.table = table;
        return this;
    }

    public List<String> getMsgs() {
        return msgs;
    }

    public CmdBuildInfo setMsgs(List<String> msgs) {
        this.msgs = msgs;
        return this;
    }

    public String getJobId() {
        return jobId;
    }

    public CmdBuildInfo setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(jobId)) {
            sb.append("jobId:").append(jobId).append("\n");
        }
        sb.append("build level:" + level).append("\n");
        if (CollectionUtils.isNotEmpty(msgs))
            sb.append(StringUtils.join(msgs, '\n'));
        if (table != null)
            sb.append(table);
        return sb.toString();
    }

    public String getSessionId() {
        return sessionId;
    }

    public CmdBuildInfo setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    /**
     * 合并
     *
     * @param other
     * @return
     */
    public CmdBuildInfo merge(CmdBuildInfo other) {
        if (other == null) return this;

        CmdBuildInfo result = new CmdBuildInfo(this.level.level > other.level.level ? this.level : other.level);
        result.addAllMsg(this.msgs);
        result.addAllMsg(other.getMsgs());

        result.setTable(this.table);
        result.setTable(other.table);

        return result;
    }
}
