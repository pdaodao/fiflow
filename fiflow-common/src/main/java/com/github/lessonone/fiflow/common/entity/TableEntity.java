package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;

import java.util.List;
import java.util.Map;


@Data
@Table("fi_table")
public class TableEntity extends BaseEntity {
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
