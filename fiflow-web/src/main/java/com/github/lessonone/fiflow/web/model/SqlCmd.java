package com.github.lessonone.fiflow.web.model;

import java.io.Serializable;

/**
 * 前端向后端传递的 sql 命令
 */
public class SqlCmd implements Serializable {
    // 待执行的sql text 为多行以;分隔的sql语句
    private String sql;
    // session id 可以为空 为空会新建一个session
    private String sessionId;

    public String getSql() {
        return sql;
    }

    public SqlCmd setSql(String sql) {
        this.sql = sql;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public SqlCmd setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }
}
