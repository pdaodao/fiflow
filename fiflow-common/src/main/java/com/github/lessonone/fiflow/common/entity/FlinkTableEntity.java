package com.github.lessonone.fiflow.common.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * fi_flink_table
 */
@Data
public class FlinkTableEntity extends BaseEntity{
    private Long databaseId;
    private Long connectorId;
    // flink table name (ddl: create table $name)
    private String name;
    // real table name / path
    private String objectName;
    private Map<String, String> properties;
    private String comment;
    private String primaryKeyName;
    private List<String> primaryKeyColumns;
    private String primaryKeyType;
    private Boolean partitioned;
    private List<String> partitionKeys;


}
