package com.github.myetl.flow.core;

import com.github.myetl.flow.core.connect.ConnectionProperties;
import com.github.myetl.flow.core.connect.JdbcProperties;
import com.github.myetl.flow.core.exception.SqlFlowConnectorBuildException;
import com.github.myetl.flow.core.exception.SqlFlowException;
import com.github.myetl.flow.core.exception.SqlFlowMetaReaderException;
import com.github.myetl.flow.core.exception.SqlFlowUnsupportException;
import com.github.myetl.flow.core.frame.*;
import com.github.myetl.flow.core.meta.BasicFieldTypeConverter;
import com.github.myetl.flow.core.meta.DBTable;
import com.github.myetl.flow.core.meta.DBTableField;
import com.github.myetl.flow.core.meta.DbInfo;
import com.github.myetl.flow.core.utils.SqlParserUtils;
import com.github.myetl.flow.core.utils.TableMetaUtils;
import com.sun.xml.internal.ws.util.MetadataUtil;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 数据元信息管理
 */
public class TableMetaStore implements Serializable {
    private List<DbInfo> dbInfoList = new ArrayList<>();


    public JdbcProperties.JdbcPropertiesBuilder jdbc() {
        return JdbcProperties.builder(this);
    }

    public TableMetaStore addDbInfo(DbInfo dbInfo) {
        this.dbInfoList.add(dbInfo);
        return this;
    }


    /**
     * 注册数据表结构 和 构建 source sink 相关
     * @param parsedTable
     * @return
     * @throws SqlFlowConnectorBuildException
     */
    protected CatalogTableImpl buildSchemaAndConnectProperties(ParsedTableList.ParsedTable parsedTable)
            throws SqlFlowException {
        final String dbName = parsedTable.getDbName();
        final String tableName = parsedTable.getTableNameDropDbName();

        DbInfo dbInfo = null;
        if(dbName == null || dbInfoList.size() == 1){
            dbInfo = dbInfoList.get(0);
        }else{
            for(DbInfo info : dbInfoList){
                if(dbName.equalsIgnoreCase(info.dbNameUsedInsql)){
                    dbInfo = info;
                    break;
                }
            }
        }

        ConnectionProperties connectionProperties = dbInfo.connectionProperties;
        if(connectionProperties == null)
            throw new SqlFlowConnectorBuildException("ConnectionProperties is null for table: "+parsedTable.tableName);

        DBTable dbTable = dbInfo.getTable(tableName);
        if(dbTable == null){
            // 读取元信息
            TableMetaReader metaReader = FactoryManager.metaReader(connectionProperties);
            List<DBTableField> fields = metaReader.fields(tableName);
            dbTable = new DBTable(tableName);
            dbTable.setFields(fields);
            dbInfo.addDBTable(dbTable);
        }

        TableSchema tableSchema = null;
        if(parsedTable.readWriteModel == ParsedTableList.ReadWriteModel.Sink){
            if(CollectionUtils.isNotEmpty(parsedTable.fields)){
                Map<String, DBTableField> fieldMap = dbTable.getFieldMap();
                List<DBTableField> fs = new ArrayList<>();
                for(String f: parsedTable.fields){
                    DBTableField fieldInfo = fieldMap.get(f);
                    if(fieldInfo == null ){
                        throw new SqlFlowMetaReaderException("unknown field "+f+" of table "+tableName);
                    }else{
                        fs.add(fieldInfo);
                    }
                }
                tableSchema = TableMetaUtils.toTableSchema(fs);
            }
        }
        if(tableSchema == null)
            tableSchema = dbTable.toSchema();;


        // properties 连接描述
        Map<String, String> properties = null;
        ConnectorBuilder connectorBuilder = FactoryManager.connectorBuilder(connectionProperties);
        if(connectorBuilder != null){
            try{
                TableConnectSetting tableConnectSetting = new TableConnectSetting(tableName);
                if(parsedTable.readWriteModel == ParsedTableList.ReadWriteModel.Source){
                    properties = connectorBuilder.buildSourceProperties(tableConnectSetting);
                }else{
                    properties = connectorBuilder.buildSinkProperties(tableConnectSetting);
                }
            } catch (SqlFlowUnsupportException e2){
                properties = null;
            }
        }

        CatalogTableImpl catalogTable = new CatalogTableImpl(tableSchema, properties, null);

        return catalogTable;
    }


    protected void registerCatalogTableInternal(ParsedTableList.ParsedTable parsedTable, Catalog catalog, EnvironmentSettings settings)
            throws SqlFlowException {

        final ObjectPath objectPath = parsedTable.getObjectPath(settings.getBuiltInDatabaseName());
        boolean isExist = catalog.tableExists(objectPath);

        if(!isExist){
            CatalogTableImpl catalogTable = buildSchemaAndConnectProperties(parsedTable);

            try {
                catalog.createTable(objectPath, catalogTable, true);
            } catch (Exception e) {
                throw new SqlFlowConnectorBuildException("catalog.createTable exception", e);
            }
        }
    }

    public void registerCatalogTableInternal(String sql, TableEnvironment tableEnv, EnvironmentSettings settings) throws SqlParseException, SqlFlowException {
       if(CollectionUtils.isEmpty(dbInfoList)){
           throw new SqlFlowConnectorBuildException("connect DBInfo is empty");
       }

        Optional<Catalog> catalogOptional = tableEnv.getCatalog(settings.getBuiltInCatalogName());
        if (!catalogOptional.isPresent())
            throw new CatalogException("catalog is null");

        final Catalog catalog = catalogOptional.get();

        final ParsedTableList parsedTableList = SqlParserUtils.getTables(sql);
        if(parsedTableList == null || CollectionUtils.isEmpty(parsedTableList.tableList)){
            return;
        }

        for(ParsedTableList.ParsedTable parsedTable: parsedTableList.tableList){
            registerCatalogTableInternal(parsedTable, catalog, settings);
        }
    }


}
