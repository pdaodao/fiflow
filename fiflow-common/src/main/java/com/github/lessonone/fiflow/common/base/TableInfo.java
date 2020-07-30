package com.github.lessonone.fiflow.common.base;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class TableInfo {
    private String name;
    private String comment;
    private List<TableColumn> columns = new ArrayList<>();

    private String primaryKeyName;
    private List<String> primaryKeyColumns = new ArrayList<>();

    public TableInfo() {
    }

    public TableInfo(String name) {
        this.name = name;
    }

    public TableInfo(String name, String comment) {
        this.name = name;
        this.comment = comment;
    }

    public TableColumn addColumn(String name,  String type, String comment){
        TableColumn column = new TableColumn();
        column.name = name;
        column.type = type;
        column.comment = comment;
        this.columns.add(column);
        return column;
    }

    public TableInfo addPrimaryKey(String pkName, String pkColumn){
        this.primaryKeyName = pkName;
        this.primaryKeyColumns.add(pkColumn);
        return this;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public String getPrimaryKeyName() {
        return primaryKeyName;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TableInfo.class.getSimpleName() + "[", "]")
                .add("name='" + name + "'")
                .add("comment='" + comment + "'")
                .add("columns=" + columns)
                .add("primaryKeyName='" + primaryKeyName + "'")
                .add("primaryKeyColumns=" + primaryKeyColumns)
                .toString();
    }

    public static class TableColumn{
        private String name;
        private String type;
        private String comment;
        private Integer size;
        private Integer digits;
        private Integer position;
        private Boolean autoincrement = false;
        private Boolean nullable;

        public String getName() {
            return name;
        }

        public TableColumn setName(String name) {
            this.name = name;
            return this;
        }

        public String getComment() {
            return comment;
        }

        public TableColumn setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public String getType() {
            return type;
        }

        public TableColumn setType(String type) {
            this.type = type;
            return this;
        }

        public Integer getPosition() {
            return position;
        }

        public TableColumn setPosition(Integer position) {
            this.position = position;
            return this;
        }

        public Integer getSize() {
            return size;
        }

        public TableColumn setSize(Integer size) {
            this.size = size;
            return this;
        }

        public Integer getDigits() {
            return digits;
        }

        public TableColumn setDigits(Integer digits) {
            this.digits = digits;
            return this;
        }

        public TableColumn setAutoincrement(Boolean autoincrement) {
            this.autoincrement = autoincrement;
            return this;
        }

        public Boolean getAutoincrement() {
            return autoincrement;
        }

        public Boolean getNullable() {
            return nullable;
        }

        public TableColumn setNullable(Boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TableColumn.class.getSimpleName() + "[", "]")
                    .add("name='" + name + "'")
                    .add("type='" + type + "'")
                    .add("comment='" + comment + "'")
                    .add("size=" + size)
                    .add("digits=" + digits)
                    .add("position=" + position)
                    .add("autoincrement=" + autoincrement)
                    .add("nullable=" + nullable)
                    .toString();
        }
    }
}
