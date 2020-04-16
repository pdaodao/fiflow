package com.github.myetl.fiflow.web.service;

import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.FiflowSqlSession;
import com.github.myetl.fiflow.web.model.SqlCmd;
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
