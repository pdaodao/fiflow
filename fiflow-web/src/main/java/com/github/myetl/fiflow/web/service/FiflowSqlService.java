package com.github.myetl.fiflow.web.service;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
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
    public CmdBuildInfo run(SqlCmd cmd) throws Exception {
        FiflowSqlSession session = fiflowService.getOrCreateSession(cmd.getSessionId());

        CmdBuildInfo buildResult = session.sql(cmd.getSql());

        buildResult.setSessionId(session.id);

        switch (buildResult.getLevel()) {
            case Select: ;
            case Insert: {
                // 提交执行
                String jobId = fiflowService.execute(session);
                buildResult.addMsg("submit job :"+jobId);
                buildResult.setJobId(jobId);
                break;
            }
            default:
                break;
        }
        return buildResult;
    }

}
