package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.frame.SessionConfig;
import com.github.myetl.fiflow.core.sql.BuildContext;
import com.github.myetl.fiflow.core.sql.SqlBuilder;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;

import java.util.ArrayList;
import java.util.List;

public class FiflowSqlSession extends FiflowSession {

    /**
     * 前端发起的一个多行的 sql 为一个 step 构建后形成一个 BuildContext
     */
    private List<BuildContext> steps = new ArrayList<>();

    public FiflowSqlSession(String id, SessionConfig sessionConfig) {
        super(id, sessionConfig);
    }

    /**
     * 把 sql 转换为 flink 中的操作 这里不提交执行
     *
     * @param sqlList 每一行为一条sql
     * @return
     */
    public FlinkBuildInfo sql(List<String> sqlList) {
        SqlBuilder sqlBuilder = new SqlBuilder(this);
        // 1. 先预处理一下 得到全局的信息
        for (String sql : sqlList) {
            sqlBuilder.preBuild(sql);
        }
        // 2. build 把 sql 转为 flink 的操作
        FlinkBuildInfo buildResult = sqlBuilder.build();
        // 3. 保存该批次的构建信息
        addStep(sqlBuilder.buildContext);
        return buildResult;
    }


    /**
     * 把多行sql文本 转为 flink 中的操作  这里不执行execution
     *
     * @param sqlText 多行以;分隔的sql语句
     * @return
     */
    public FlinkBuildInfo sql(String sqlText) {
        // 切分多行sql
        List<String> sqls = SqlSplitUtil.split(sqlText);
        return sql(sqls);
    }

    public FiflowSqlSession addStep(BuildContext buildContext) {
        this.steps.add(buildContext);
        return this;
    }

    public List<BuildContext> getSteps() {
        return steps;
    }
}
