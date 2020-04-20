package com.github.myetl.fiflow.core.flink;

import com.github.myetl.fiflow.core.pojo.TableData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * sql/flow 操作转为 flink 操作的构建 结果
 */
public class FlinkBuildInfo implements Serializable {
    // 构建级别
    private final BuildLevel level;
    // 信息
    private List<String> msgs;
    // 构建过程中生产的表格数据 如 help, show tables 等操作返回的数据
    private TableData table;

    // FiflowSession id
    private String sessionId;
    // SessionContext id
    private String contextId;
    // submit job id
    private String jobId;

    public FlinkBuildInfo(BuildLevel level) {
        this.level = level;
    }

    public static FlinkBuildInfo of(BuildLevel level, TableData rowSet) {
        FlinkBuildInfo r = new FlinkBuildInfo(level);
        r.setTable(rowSet);
        return r;
    }

    public FlinkBuildInfo addMsg(String msg) {
        if (StringUtils.isEmpty(msg)) return this;
        if (msgs == null) msgs = new ArrayList<>();
        msgs.add(msg);
        return this;
    }

    public FlinkBuildInfo addAllMsg(List<String> msg) {
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

    public FlinkBuildInfo setTable(TableData table) {
        if (table == null) return this;
        this.table = table;
        return this;
    }

    public TableData table() {
        if (table == null) {
            table = TableData.instance();
        }
        return table;
    }


    public List<String> getMsgs() {
        return msgs;
    }

    public FlinkBuildInfo setMsgs(List<String> msgs) {
        this.msgs = msgs;
        return this;
    }


    public String getSessionId() {
        return sessionId;
    }

    public FlinkBuildInfo setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public String getContextId() {
        return contextId;
    }

    public FlinkBuildInfo setContextId(String contextId) {
        this.contextId = contextId;
        return this;
    }

    public String getJobId() {
        return jobId;
    }

    public FlinkBuildInfo setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(getJobId())) {
            sb.append("jobId:").append(getJobId()).append("\n");
        }
        sb.append("build level:" + level).append("\n");
        if (CollectionUtils.isNotEmpty(msgs))
            sb.append(StringUtils.join(msgs, '\n'));
        if (table != null)
            sb.append(table);
        return sb.toString();
    }


    /**
     * 合并
     *
     * @param other
     * @return
     */
    public FlinkBuildInfo merge(FlinkBuildInfo other) {
        if (other == null) return this;
        FlinkBuildInfo result = new FlinkBuildInfo(this.level.level > other.level.level ? this.level : other.level);
        result.addAllMsg(this.msgs);
        result.addAllMsg(other.getMsgs());

        result.setTable(this.table);
        result.setTable(other.table);

        return result;
    }
}
