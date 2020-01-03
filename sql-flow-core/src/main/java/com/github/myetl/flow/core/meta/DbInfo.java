package com.github.myetl.flow.core.meta;

import com.github.myetl.flow.core.connect.ConnectionProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.CollectionUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库信息 抽象 相当于 mysql 中的一个数据库
 * 内部含有多个数据表信息
 */
public class DbInfo implements Serializable {
    /**
     * 数据库连接信息
     */
    public ConnectionProperties connectionProperties;

    public String dbNameUsedInsql;

    /**
     * 数据表列表
     */
    private List<DBTable> tableList = new ArrayList<>();

    public DbInfo() {
    }

    public DbInfo(ConnectionProperties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public DBTable.DbTableBuilder table(String tableName) {
        return DBTable.builder(this, tableName);
    }

    public DbInfo addDBTable(DBTable dbTable) {
        this.tableList.add(dbTable);
        return this;
    }

    public List<DBTable> getTableList() {
        return tableList;
    }

    public DbInfo setDbNameUsedInsql(String dbNameUsedInsql) {
        this.dbNameUsedInsql = dbNameUsedInsql;
        return this;
    }

    public DBTable getTable(String tableName) {
        if(StringUtils.isEmpty(tableName)) return null;
        if(CollectionUtils.isEmpty(tableList)) return null;
        for(DBTable table: tableList){
            if(tableName.equalsIgnoreCase(table.getName())){
                return table;
            }
        }
        return null;
    }
}
