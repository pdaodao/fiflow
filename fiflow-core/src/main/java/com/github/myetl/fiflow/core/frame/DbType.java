package com.github.myetl.fiflow.core.frame;

/**
 * 数据库类型
 */
public enum  DbType {
    Jdbc,
    Hive,
    Kafka,
    Hbase,
    Elasticsearch,
    Csv,
    Binlog;
}
