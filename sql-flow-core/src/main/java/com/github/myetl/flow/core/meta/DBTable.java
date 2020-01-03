package com.github.myetl.flow.core.meta;

import com.github.myetl.flow.core.exception.SqlFlowMetaFieldTypeException;
import com.github.myetl.flow.core.utils.TableMetaUtils;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据库表信息
 */
public class DBTable implements Serializable {
    private String name;
    private String alias;
    private String comment;
    private List<DBTableField> fields = new ArrayList<>();

    public DBTable() {
    }

    public DBTable(String name) {
        this.name = name;
    }

    public static DbTableBuilder builder(DbInfo dbInfo, String tableName) {
        return new DbTableBuilder(dbInfo, tableName);
    }

    public String getComment() {
        return comment;
    }

    public DBTable setComment(String comment) {
        this.comment = comment;
        return this;
    }

    public String getName() {
        return name;
    }

    public DBTable addField(DBTableField field) {
        if (field == null) return this;
        this.fields.add(field);
        return this;
    }

    public List<DBTableField> getFields() {
        return fields;
    }

    public DBTable setFields(List<DBTableField> fields) {
        if (fields == null) return this;
        this.fields.addAll(fields);
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public DBTable setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public TableSchema toSchema() throws SqlFlowMetaFieldTypeException {
        return TableMetaUtils.toTableSchema(fields);
    }

    @Override
    public String toString() {
        return name;
    }

    public Map<String, DBTableField> getFieldMap(){
        Map<String, DBTableField> map = new CaseInsensitiveMap();
        for(DBTableField field: fields){
            map.put(field.getName(), field);
        }
        return map;
    }

    public static class DbTableBuilder {
        final DbInfo dbInfo;
        final DBTable dbTable;

        public DbTableBuilder(DbInfo dbInfo, String tableName) {
            this.dbInfo = dbInfo;
            this.dbTable = new DBTable(tableName);
        }

        public DbInfo build() {
            return dbInfo;
        }
    }
}
