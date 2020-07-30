package com.github.lessonone.fiflow.common.entity;

/**
 * table fi_flink_table_column
 */
public class FlinkColumnEntity {
    public static final String TableName = "fi_flink_table_column";
    public static final String SqlSelectByTableId = "SELECT id, table_id, name ,data_type, expr FROM " + TableName + " WHERE table_id = ?";
    public static final String SqlDeleteByTableId = "DELETE FROM " + TableName + " WHERE table_id = ?";

    private Long id;
    // FlinkTableEntity id
    private Long tableId;
    private String name;
    // flink column data type
    private String dataType;
    private String expr;

    public Long getId() {
        return id;
    }

    public FlinkColumnEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public Long getTableId() {
        return tableId;
    }

    public FlinkColumnEntity setTableId(Long tableId) {
        this.tableId = tableId;
        return this;
    }

    public String getName() {
        return name;
    }

    public FlinkColumnEntity setName(String name) {
        this.name = name;
        return this;
    }

    public String getDataType() {
        return dataType;
    }

    public FlinkColumnEntity setDataType(String dataType) {
        this.dataType = dataType;
        return this;
    }

    public String getExpr() {
        return expr;
    }

    public FlinkColumnEntity setExpr(String expr) {
        this.expr = expr;
        return this;
    }
}
