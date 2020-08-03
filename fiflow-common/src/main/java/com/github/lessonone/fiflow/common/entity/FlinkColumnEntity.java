package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;

/**
 * fi_flink_table_column
 */
@Data
@Table("fi_flink_table_column")
public class FlinkColumnEntity extends BaseEntity {
    private Long tableId;
    private String name;
    // flink column data type
    private String dataType;
    private String expr;
}
