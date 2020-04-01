package com.github.myetl.fiflow.core.flink;

import java.io.Serializable;

/**
 * flink  集群信息
 */
public class FlinkClusterInfo implements Serializable {
    // 用户自定义的唯一名称方便使用
    private String code;

    private String host = "127.0.0.1";

    private Integer port = 8081;

    // 集群模式
    private FlinkMode mode = FlinkMode.yarn;

    public String getCode() {
        return code;
    }

    public FlinkClusterInfo setCode(String code) {
        this.code = code;
        return this;
    }

    public String getHost() {
        return host;
    }

    public FlinkClusterInfo setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public FlinkClusterInfo setPort(Integer port) {
        this.port = port;
        return this;
    }

    public FlinkMode getMode() {
        return mode;
    }

    public FlinkClusterInfo setMode(FlinkMode mode) {
        this.mode = mode;
        return this;
    }
}
