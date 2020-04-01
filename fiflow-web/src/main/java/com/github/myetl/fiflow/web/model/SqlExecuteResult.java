package com.github.myetl.fiflow.web.model;

import java.io.Serializable;

public class SqlExecuteResult implements Serializable {
    private String sessionId;
    private String msg;

    public String getSessionId() {
        return sessionId;
    }

    public SqlExecuteResult setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public SqlExecuteResult setMsg(String msg) {
        this.msg = msg;
        return this;
    }
}
