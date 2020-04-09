package com.github.myetl.fiflow.core.pojo;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 表格数据
 * sql 执行结果 表头 和 多行数据
 */
public class TableData implements Serializable {
    // 字段名称
    private List<String> heads;
    // 字段类型
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<String> types;
    // 多行数据
    private List<TableRow> rows;

    public static TableData instance() {
        return new TableData();
    }

    public void addHeads(String... names) {
        if (heads == null) {
            heads = new ArrayList<>();
        }
        for (String name : names) {
            heads.add(name);
        }
    }

    public void addRow(TableRow row) {
        if (row == null) return;
        if (rows == null)
            rows = new ArrayList<>();
        rows.add(row);
    }

    public void addRow(String... rowValues) {
        this.addRow(TableRow.of(rowValues));
    }

    public List<String> getHeads() {
        return heads;
    }

    public TableData setHeads(List<String> head) {
        this.heads = head;
        return this;
    }

    public List<TableRow> getRows() {
        return rows;
    }

    public TableData setRows(List<TableRow> rows) {
        this.rows = rows;
        return this;
    }

    public List<String> getTypes() {
        return types;
    }

    public TableData setTypes(List<String> types) {
        this.types = types;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(StringUtils.join(heads, ',')).append("\n");
        sb.append(StringUtils.repeat('-', sb.length())).append("\n");

        sb.append(StringUtils.join(rows, '\n'));

        return sb.toString();
    }
}
