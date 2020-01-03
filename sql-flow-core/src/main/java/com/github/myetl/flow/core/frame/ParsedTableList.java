package com.github.myetl.flow.core.frame;

import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.ArrayList;
import java.util.List;

public class ParsedTableList {
    public List<ParsedTable> tableList = new ArrayList<>();

    public ParsedTableList add(String tableName, ReadWriteModel readWriteModel){
        tableList.add(new ParsedTable(tableName, readWriteModel));
        return this;
    }

    public ParsedTableList add(ParsedTable table){
        tableList.add(table);
        return this;
    }

    public ParsedTableList add(String tableName, ReadWriteModel readWriteModel, SqlNode where){
        tableList.add(new ParsedTable(tableName, readWriteModel, where));
        return this;
    }


    public static enum ReadWriteModel{
        Source,
        Sink;
    }

    public static class ParsedTable {
        public String tableName;
        public List<String> fields = new ArrayList<>();

        public ReadWriteModel readWriteModel;
        public SqlNode where;

        public ParsedTable addField(String field){
            this.fields.add(field);
            return this;
        }

        public String getDbName(){
            if(tableName.indexOf(".") > 0) return tableName.substring(0, tableName.indexOf("."));
            return null;
        }

        public String getTableNameDropDbName(){
            if(tableName.indexOf(".") > 0) return tableName.substring(tableName.indexOf(".")+1);
            return tableName;
        }

        public ObjectPath getObjectPath(String defaultDbName) {
            String dbName = defaultDbName;
            String[] tableNameArr = tableName.split(".");
            if (tableNameArr.length == 2) {
                dbName = tableNameArr[0];
                tableName = tableNameArr[1];
            }
            return  new ObjectPath(dbName, tableName);
        }

        public ParsedTable(String tableName) {
            this.tableName = tableName;
        }

        public ParsedTable(String tableName, ReadWriteModel readWriteModel) {
            this.tableName = tableName;
            this.readWriteModel = readWriteModel;
        }

        public ParsedTable(String tableName, ReadWriteModel readWriteModel, SqlNode where) {
            this.tableName = tableName;
            this.readWriteModel = readWriteModel;
            this.where = where;
        }
    }
}
