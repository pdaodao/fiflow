package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.frame.SessionConfig;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.SqlToFlinkBuilder;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;

import java.util.List;

public class FiflowSqlSession extends FiflowSession {

    public FiflowSqlSession(String id, SessionConfig sessionConfig) {
        super(id, sessionConfig);
    }


    /**
     * 把 sqlText 转换成 flink 中的 sql 操作 这里不执行execution
     *
     * @param sqlText 多行以;分隔的sql语句
     */
    public FlinkBuildInfo sql(String sqlText) {
        // 切分多行sql
        List<String> sqls = SqlSplitUtil.split(sqlText);

        FlinkBuildInfo buildResult = new FlinkBuildInfo(BuildLevel.None);
        for (String sql : sqls) {
            FlinkBuildInfo step;
            try {
                step = SqlToFlinkBuilder.build(sql, this);
            } catch (Exception e) {
                step = new FlinkBuildInfo(BuildLevel.Error);
                step.addMsg(e.getMessage());
            }
            buildResult = buildResult.merge(step);
            if (buildResult.getLevel() == BuildLevel.Error)
                return buildResult;
        }
        return buildResult;
    }
}
