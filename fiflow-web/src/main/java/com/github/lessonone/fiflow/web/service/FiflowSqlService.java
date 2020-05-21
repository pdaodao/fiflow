package com.github.lessonone.fiflow.web.service;

import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.FiflowSqlSession;
import com.github.lessonone.fiflow.web.model.SqlCmd;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 使用 fiflow 在 flink 中执行 sql
 */
@Service
public class FiflowSqlService {
    @Autowired
    private FiflowService fiflowService;

    /**
     * todo
     *
     * @param cmd
     * @throws Exception
     */
    public FlinkBuildInfo run(SqlCmd cmd) throws Exception {
        FiflowSqlSession session = fiflowService.getOrCreateSession(cmd.getSessionId());

        return session.sql(cmd.getSql());

    }

}
