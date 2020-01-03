package com.github.myetl.flow.core.meta;

import java.io.Serializable;

/**
 * 字段
 */
public class DBTableField implements Serializable {
    private String name;
    private DbFieldType fieldType;
    private String comment;
    private String alias;


    public DBTableField() {
    }

    public DBTableField(String name) {
        this.name = name;
    }

    public DBTableField(String name, DbFieldType dbFieldType) {
        this.name = name;
        this.fieldType = dbFieldType;
    }


    public String getName() {
        return name;
    }

    public DBTableField setName(String name) {
        this.name = name;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public DBTableField setComment(String comment) {
        this.comment = comment;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public DBTableField setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public DbFieldType getFieldType() {
        return fieldType;
    }

    public DBTableField setFieldType(DbFieldType fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        if (fieldType != null) {
            sb.append(" ").append(fieldType.toString());
        }
        return sb.toString();
    }
}

