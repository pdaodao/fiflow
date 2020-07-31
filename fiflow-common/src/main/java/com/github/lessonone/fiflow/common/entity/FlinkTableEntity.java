package com.github.lessonone.fiflow.common.entity;

/**
 * fi_flink_table
 */
public class FlinkTableEntity {


    private Long id;
    private Long databaseId;
    private Long connectorId;
    // flink table name (ddl: create table $name)
    private String name;
    // real table name / path
    private String objectName;
    private String properties;
    private String comment;
    private String primaryKeyName;
    private String primaryKeyColumns;
    private String primaryKeyType;
    private Boolean partitioned;
    private String partitionKeys;


}
