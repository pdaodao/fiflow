package com.github.myetl.flow.core.connect;

public enum ConnectorType {
    jdbc,
    csv,
    elasticsearch,
    kafka,
    hive;

    public static ConnectorType from(String type) {
        return ConnectorType.valueOf(type.toLowerCase());
    }
}
