package com.github.lessonone.fiflow.common.base;

@Deprecated
public enum ConnectorType {
    jdbc,
    mysql,
    kafka,
    postgresql,
    elasticsearch,
    oracle,
    gbase,
    sqlserver,
    hive,
    ads,
    odps,
    hbase;

    public static ConnectorType parse(String name){
        return ConnectorType.valueOf(name.toLowerCase());
    }
}
