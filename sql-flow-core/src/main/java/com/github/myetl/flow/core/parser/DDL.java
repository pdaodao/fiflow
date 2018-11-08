package com.github.myetl.flow.core.parser;

import com.github.myetl.flow.core.enums.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * create table 中的数据表的定义
 */
public class DDL {
    /**
     * 数据表名称
     */
    private final String tableName;

    /**
     * 字段信息
     */
    private final List<DDLFieldInfo> fields = new ArrayList<>();

    /**
     * 主键
     */
    private List<String> primaryKeys;

    /**
     * 存储类型 mysql,hbase ...
     */
    private String type;

    /**
     * with 部分的 属性
     */
    private Map<String, Object> props;


    public DDL(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public DDLFieldInfo addField(String fieldName, FieldType type) {
        DDLFieldInfo fieldInfo = new DDLFieldInfo(fieldName, type);
        this.fields.add(fieldInfo);
        return fieldInfo;
    }

    public List<DDLFieldInfo> getFields() {
        return fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public DDL setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public String getType() {
        return type;
    }

    public DDL setType(String type) {
        this.type = type;
        return this;
    }

    public Map<String, Object> getProps() {
        return props;
    }

    public DDL setProps(Map<String, Object> props) {
        this.props = props;
        return this;
    }

    /**
     * 字段信息
     */
    public static class DDLFieldInfo {
        private final String name;
        private final FieldType type;

        public DDLFieldInfo(String name, FieldType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public FieldType getType() {
            return type;
        }
    }
}

