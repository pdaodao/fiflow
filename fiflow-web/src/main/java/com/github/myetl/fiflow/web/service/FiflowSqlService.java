package com.github.myetl.fiflow.web.service;

import com.github.myetl.fiflow.core.frame.FiFlinkSession;
import com.github.myetl.fiflow.core.sql.SqlBuildContext;
import com.github.myetl.fiflow.runtime.FiflowFlinkExecutor;
import com.github.myetl.fiflow.web.model.SqlCmd;
import com.github.myetl.fiflow.web.model.SqlExecuteResult;
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
     * @param cmd
     * @throws Exception
     */
    public SqlExecuteResult run(SqlCmd cmd) throws Exception{
        FiFlinkSession session = fiflowService.getOrCreateSession(cmd.getSessionId());

        SqlBuildContext sqlBuildContext = session.sql(cmd.getSql());

        System.out.println("session id:"+session.getId());

        if(sqlBuildContext.needExecute){
//            FiflowRunner.executeLocal(session);
            FiflowFlinkExecutor.executeStandalone(session, null);
        }
        SqlExecuteResult ret = new SqlExecuteResult();
        ret.setSessionId(session.getId());

        return ret;
    }

}
