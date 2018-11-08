package com.github.myetl.flow.core.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * INSERT INTO tableName [ (columnName[ , columnName]*) ] queryStatement;
 */
public class DML {

    private final String sql;
    /**
     * insert into table name
     */
    private String targetTable;

    private List<String> sourceTable = new ArrayList<>();

    /**
     * insert into fields
     */
    private List<String> targetFields;

    public DML(String sql) {
        this.sql = sql;
    }

    public DML addSourceTable(String tableName) {
        this.sourceTable.add(tableName);
        return this;
    }


    public List<String> getSourceTable() {
        return sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public DML setTargetTable(String targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public List<String> getTargetFields() {
        return targetFields;
    }

    public DML setTargetFields(List<String> targetFields) {
        this.targetFields = targetFields;
        return this;
    }

}
