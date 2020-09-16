package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;

/**
 * 字段信息
 */
@Data
@Table("fi_table_column")
public class ColumnEntity extends BaseEntity {
    private Long tableId;
    private String name;
    // flink column data type
    private String dataType;
    private String expr;
    private Integer position;
}
