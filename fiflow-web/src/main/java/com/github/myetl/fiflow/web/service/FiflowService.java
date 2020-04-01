package com.github.myetl.fiflow.web.service;

import com.github.myetl.fiflow.core.frame.FiFlinkSession;
import com.github.myetl.fiflow.core.frame.FlowSessionManager;
import com.github.myetl.fiflow.core.frame.SessionConfig;
import org.springframework.stereotype.Service;

@Service
public class FiflowService {

    /**
     * 获取或者创建 session
     * @param sessionId
     * @return
     */
    public FiFlinkSession getOrCreateSession(String sessionId) {
        SessionConfig sessionConfig = new SessionConfig();

        FiFlinkSession session = FlowSessionManager.getOrCreateSession(sessionId, sessionConfig);

        return session;
    }
}
