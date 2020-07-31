package com.github.lessonone.fiflow.common.entity;

import lombok.Data;

/**
 * fi_flink_table_column
 */
@Data
public class FlinkColumnEntity extends BaseEntity {
    private Long tableId;
    private String name;
    // flink column data type
    private String dataType;
    private String expr;
}
