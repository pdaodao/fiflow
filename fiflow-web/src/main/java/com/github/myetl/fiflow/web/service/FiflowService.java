package com.github.myetl.fiflow.web.service;

import com.github.myetl.fiflow.core.core.FiflowRuntime;
import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.frame.FiflowRuntimeLoader;
import com.github.myetl.fiflow.core.frame.FlowSessionManager;
import com.github.myetl.fiflow.core.frame.JobSubmitResult;
import com.github.myetl.fiflow.core.frame.SessionConfig;
import org.springframework.stereotype.Service;

@Service
public class FiflowService {

    /**
     * 获取或者创建 session
     *
     * @param sessionId
     * @return
     */
    public FiflowSqlSession getOrCreateSession(String sessionId) {
        SessionConfig sessionConfig = new SessionConfig();

        FiflowSqlSession session = FlowSessionManager.getOrCreateSession(sessionId, sessionConfig);

        return session;
    }

    /**
     * 提交到 flink 集群执行
     *
     * @param fiflowSqlSession
     * @return jobId
     * @throws Exception
     */
    public String execute(FiflowSqlSession fiflowSqlSession) throws Exception {
        FiflowRuntime fiflowRuntime = FiflowRuntimeLoader.getRuntime();

        JobSubmitResult jobSubmitResult = fiflowRuntime.submit(fiflowSqlSession);

        return jobSubmitResult.getJobId();
    }
}
