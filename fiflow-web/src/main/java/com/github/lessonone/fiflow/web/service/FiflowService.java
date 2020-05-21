package com.github.lessonone.fiflow.web.service;

import com.github.lessonone.fiflow.core.frame.FlowSessionManager;
import com.github.lessonone.fiflow.core.frame.SessionConfig;
import com.github.lessonone.fiflow.core.sql.FiflowSqlSession;
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
}
