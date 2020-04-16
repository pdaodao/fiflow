package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.SessionContext;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SqlSessionContext extends SessionContext {
    public final EnvironmentSettings settings;
    public final StreamTableEnvironment tEnv;
    private List<Cmd> cmdList = new ArrayList<>();

    public SqlSessionContext(String id, StreamExecutionEnvironment env, EnvironmentSettings settings, StreamTableEnvironment tEnv) {
        super(id, env);
        if (settings == null) {
            settings = EnvironmentSettings
                    .newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner()
                    .build();
        }
        if (tEnv == null) {
            tEnv = StreamTableEnvironment.create(this.env, settings);
        }
        this.settings = settings;
        this.tEnv = tEnv;
    }

    public static SqlSessionContext create(String id, SqlSessionContext previous) {
        if (previous == null) {
            return create(id);
        }
        return new SqlSessionContext(id, previous.env, previous.settings, previous.tEnv);
    }

    public static SqlSessionContext create(String id) {
        return new SqlSessionContext(id, null, null, null);
    }

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

    public SqlSessionContext setCmdList(List<Cmd> cmdList) {
        this.cmdList = cmdList;
        return this;
    }

    /**
     * 获取等级最高的 level
     *
     * @return
     */
    public BuildLevel level() {
        BuildLevel level = BuildLevel.None;
        if (CollectionUtils.isEmpty(cmdList)) return level;
        for (Cmd cmd : cmdList) {
            if (cmd.level().level > level.level) {
                level = cmd.level();
            }
        }
        return level;
    }
}
