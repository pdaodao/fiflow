package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.SessionContext;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.util.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

public final class SqlSessionContext extends SessionContext {
    public final EnvironmentSettings settings;
    public final StreamTableEnvironment tEnv;
    public final List<Cmd> cmdList;

    private SqlSessionContext(String id, List<Cmd> cmdList, FiflowSqlSession session, StreamExecutionEnvironment env,
                              EnvironmentSettings settings, StreamTableEnvironment tEnv) {
        super(id, session, env);
        this.cmdList = new ArrayList<>();
        if (cmdList != null) {
            this.cmdList.addAll(cmdList);
        }

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


    public static SqlSessionContext create(String id, List<Cmd> cmdList, FiflowSqlSession session) {
        Preconditions.checkNotNull(session, "FiflowSqlSession is null");
        StreamExecutionEnvironment env = null;
        EnvironmentSettings settings = null;
        StreamTableEnvironment tEnv = null;
        for (SqlSessionContext ct : session.getContextList()) {
            if (ct.env != null) env = ct.env;
            if (ct.settings != null) settings = ct.settings;
            if (ct.tEnv != null) tEnv = ct.tEnv;
        }
        return new SqlSessionContext(id, cmdList, session, env, settings, tEnv);
    }

    /**
     * 自动生成 id
     *
     * @param session
     * @return
     */
    public static SqlSessionContext create(List<Cmd> cmdList, FiflowSqlSession session) {
        return create(null, cmdList, session);
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

    /**
     * 把 cmd 命令 转成 flink 操作
     *
     * @return
     */
    @Override
    public FlinkBuildInfo build() {
        super.build();
        FlinkBuildInfo buildInfo = new FlinkBuildInfo(BuildLevel.None);
        for (Cmd cmd : cmdList) {
            FlinkBuildInfo cmdBuildInfo = cmd.build(this);
            buildInfo = buildInfo.merge(cmdBuildInfo);
            if (buildInfo.getLevel() == BuildLevel.Error)
                return buildInfo;
        }
        session.addContext(this);

        buildInfo.setSessionId(session.id);
        buildInfo.setContextId(id);
        return buildInfo;
    }

    @Override
    protected boolean isNeedSubmit() {
        return level().isNeedExecute();
    }
}
