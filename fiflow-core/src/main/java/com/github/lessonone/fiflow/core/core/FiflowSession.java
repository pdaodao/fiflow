package com.github.lessonone.fiflow.core.core;

import com.github.lessonone.fiflow.core.frame.SessionConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 核心 入口
 */
public abstract class FiflowSession<T extends SessionContext> {
    public final String id;
    public final SessionConfig sessionConfig;
    public volatile Boolean closed = false;
    private List<T> contextList = new ArrayList<>();

    private AtomicInteger step = new AtomicInteger(0);

    public FiflowSession(String id, SessionConfig sessionConfig) {
        this.id = id;
        if (sessionConfig == null) {
            sessionConfig = new SessionConfig();
        }
        this.sessionConfig = sessionConfig;
    }

    /**
     * context build 成功后 才加入到 session 中
     *
     * @param conetxt
     * @return
     */
    public FiflowSession addContext(T conetxt) {
        this.contextList.add(conetxt);
        return this;
    }

    public List<T> getContextList() {
        return contextList;
    }

    public String incrementAndGetContextId() {
        return this.id + "-" + step.incrementAndGet();
    }

    /**
     * 关闭该 session
     */
    public synchronized void close() {
        this.closed = true;
    }

}
