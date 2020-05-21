package com.github.lessonone.fiflow.core.sql;

import com.github.lessonone.fiflow.core.core.FiflowSession;
import com.github.lessonone.fiflow.core.core.SessionContext;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.frame.SessionConfig;
import com.github.lessonone.fiflow.core.util.SqlSplitUtil;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class FiflowSqlSession extends FiflowSession<SqlSessionContext> {

    public FiflowSqlSession(String id, SessionConfig sessionConfig) {
        super(id, sessionConfig);
    }

    /**
     * 把 sql 转换为 flink 中的操作 这里不提交执行
     *
     * @param sqlList 每一行为一条sql
     * @return
     */
    public FlinkBuildInfo sql(List<String> sqlList) throws Exception {
        SqlBuilder sqlBuilder = new SqlBuilder(this, sqlList);

        Tuple2<FlinkBuildInfo, SqlSessionContext> ret = sqlBuilder.build();

        SessionContext context = ret.f1;
        FlinkBuildInfo buildResult = ret.f0;


        switch (buildResult.getLevel()) {
            case Select:
            case Insert: {
                // 提交执行
                String jobId = context.submit().getJobId();
                buildResult.setJobId(jobId);
                buildResult.addMsg("submit job :" + jobId);
                break;
            }
            default:
                break;
        }

        return buildResult;
    }


    /**
     * 把多行sql文本 转为 flink 中的操作  这里不执行execution
     *
     * @param sqlText 多行以;分隔的sql语句
     * @return
     */
    public FlinkBuildInfo sql(String sqlText) throws Exception {
        // 切分多行sql
        List<String> sqls = SqlSplitUtil.split(sqlText);
        return sql(sqls);
    }
}
