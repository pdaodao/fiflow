package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * sql command 转化为 flink 的操作
 */
public class SqlBuilder extends CmdListImpl implements CmdList {
    public final FiflowSqlSession session;
    public final List<String> sqlList;
    public final SqlSessionContext previousContext;


    public SqlBuilder(FiflowSqlSession session, List<String> sqlList) {
        this.session = session;
        this.sqlList = sqlList;

        SqlSessionContext lastContext = null;
        List<Cmd> cmdList = new ArrayList<>();
        for (SqlSessionContext context : session.getContextList()) {
            cmdList.addAll(context.getCmdList());
            lastContext = context;
        }
        String lastId = "previous";
        previousContext = SqlSessionContext.create(lastId, lastContext);
        previousContext.addCmdAll(cmdList);
    }

    /**
     * 预处理
     *
     * @param sql 单条 sql
     */
    private void preBuild(String sql) {
        if (StringUtils.isBlank(sql)) return;
        sql = sql.trim();
        for (CmdType cmdType : CmdType.values()) {
            Optional<String[]> accept = cmdType.cmdBuilder.accept(sql);
            if (accept.isPresent()) {
                Cmd cmd = new Cmd(cmdType, accept.get());
                cmd.preBuild(this);
                addCmd(cmd);
                return;
            }
        }
        throw new IllegalArgumentException("unknown sql type: " + sql);
    }


    /**
     * 先预构建下 得到全局的信息
     *
     * @return
     */
    private void preBuild() {
        // 1. 先预处理一下 得到全局的信息
        for (String sql : sqlList) {
            preBuild(sql);
        }
    }

    /**
     * 初始化环境
     * 这里利用得到的全局信息 创建 或者 继承 context
     * 还可以做一些优化 比如 谓词下推等
     * 如果环境改变需要 重放 set 和 create 操作 来构造环境
     *
     * @return
     */
    private SqlSessionContext initContext() {
        SqlSessionContext context = SqlSessionContext.create(session.incrementAndGetContextId(), this.previousContext);
        context.addCmdAll(getCmdList());
        return context;
    }

    /**
     * 预处理 > 初始化环境 > 转换到 flink 操作
     *
     * @return
     */
    public Tuple2<FlinkBuildInfo, SqlSessionContext> build() {
        //1. 预处理
        preBuild();
        //2. 初始化环境
        final SqlSessionContext sessionContext = initContext();
        //3. 转换到 flink 操作

        FlinkBuildInfo buildInfo = new FlinkBuildInfo(BuildLevel.None);

        for (Cmd cmd : getCmdList()) {
            FlinkBuildInfo cmdBuildInfo = cmd.build(sessionContext);
            buildInfo = buildInfo.merge(cmdBuildInfo);
            if (buildInfo.getLevel() == BuildLevel.Error)
                return new Tuple2(buildInfo, null);
        }

        session.addContext(sessionContext);
        buildInfo.setContextId(sessionContext.id);
        buildInfo.setSessionId(session.id);

        return new Tuple2(buildInfo, sessionContext);
    }
}
