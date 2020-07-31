package com.github.lessonone.fiflow.common;

import com.github.lessonone.fiflow.common.base.DbInfo;
import com.github.lessonone.fiflow.common.base.TableInfo;
import com.github.lessonone.fiflow.common.catalog.FlinkCatalogDatabase;
import com.github.lessonone.fiflow.common.exception.AutoMetaNotSupportException;
import com.github.lessonone.fiflow.common.meta.DispatchMetaReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.util.StringUtils;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 直接从物理数据库中读取数据表结构信息 转换成 flink 需要的 ddl 信息
 */
public class FlinkMetaCatalog extends AbstractCatalog {
    public static final String DEFAULT_DB = "default";
    private final Catalog backedCatalog;
    private final Map<String, DbInfo> databases;
    private final DispatchMetaReader dispatchMetaReader;

    public FlinkMetaCatalog(String name, String defaultDatabase) {
        super(name, defaultDatabase);
        this.backedCatalog = new GenericInMemoryCatalog(name, defaultDatabase);
        this.databases = new LinkedHashMap<>();
        this.dispatchMetaReader = new DispatchMetaReader();
    }

    public FlinkMetaCatalog(String name) {
        this(name, DEFAULT_DB);
    }

    public void addDbInfo(String flinkDatabaseName, DbInfo dbInfo) throws DatabaseAlreadyExistException {
        if (databases.containsKey(flinkDatabaseName)) {
            throw new DatabaseAlreadyExistException(getName(), flinkDatabaseName);
        }
        databases.put(flinkDatabaseName, dbInfo);
    }

    @Override
    public void open() throws CatalogException {
        backedCatalog.open();
    }

    @Override
    public void close() throws CatalogException {
        backedCatalog.close();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        Set<String> ds = new LinkedHashSet<>(backedCatalog.listDatabases());
        ds.addAll(databases.keySet());
        return new ArrayList<>(ds);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        if (backedCatalog.databaseExists(databaseName)) {
            return backedCatalog.getDatabase(databaseName);
        }
        return new FlinkCatalogDatabase(null, new HashMap<>(), null).setCatalog(getName());
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        if (this.databases.containsKey(databaseName))
            return true;
        if (backedCatalog.databaseExists(databaseName))
            return true;
        return false;
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        backedCatalog.createDatabase(name, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        backedCatalog.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        backedCatalog.alterDatabase(name, newDatabase, ignoreIfNotExists);
    }


    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> tables = new ArrayList<>();
        if (backedCatalog.databaseExists(databaseName)) {
            tables.addAll(backedCatalog.listTables(databaseName));
        }
        if (this.databases.containsKey(databaseName)) {
            try {
                tables.addAll(dispatchMetaReader.getMetaReader(this.databases.get(databaseName)).listTables());
            } catch (AutoMetaNotSupportException e) {
                if (CollectionUtils.isNotEmpty(tables)) return tables;
                throw e;
            }
        }
        return tables;
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (backedCatalog.tableExists(tablePath)) {
            return backedCatalog.getTable(tablePath);
        }
        if (!this.databases.containsKey(tablePath.getDatabaseName())) {
            throw new TableNotExistException(getName(), tablePath, new DatabaseNotExistException(getName(), tablePath.getDatabaseName()));
        }
        return dispatchMetaReader.getTable(this.databases.get(tablePath.getDatabaseName()), tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        if (backedCatalog.tableExists(tablePath)) {
            return true;
        }
        if (!this.databases.containsKey(tablePath.getDatabaseName())) {
            return false;
        }
        TableInfo tableInfo = dispatchMetaReader.getMetaReader(databases.get(tablePath.getDatabaseName()))
                .getTable(tablePath.getObjectName());
        if (tableInfo != null && CollectionUtils.isNotEmpty(tableInfo.getColumns())) {
            return true;
        }
        return false;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        if (backedCatalog.tableExists(tablePath)) {
            backedCatalog.dropTable(tablePath, ignoreIfNotExists);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        if(backedCatalog.tableExists(tablePath)){
            backedCatalog.renameTable(tablePath, newTableName, ignoreIfNotExists);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        backedCatalog.createTable(tablePath, table, ignoreIfExists);
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        if (backedCatalog.tableExists(tablePath)) {
            backedCatalog.alterTable(tablePath, newTable, ignoreIfNotExists);
            return;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {

    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return backedCatalog.listFunctions(dbName);
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        return backedCatalog.getFunction(functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return backedCatalog.functionExists(functionPath);
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        backedCatalog.createFunction(functionPath, function, ignoreIfExists);
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        backedCatalog.alterFunction(functionPath, newFunction, ignoreIfNotExists);
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        backedCatalog.dropFunction(functionPath, ignoreIfNotExists);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {

    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }
}
