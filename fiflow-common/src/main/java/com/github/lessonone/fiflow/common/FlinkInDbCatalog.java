package com.github.lessonone.fiflow.common;

import com.github.lessonone.fiflow.common.base.DbInfo;
import com.github.lessonone.fiflow.common.entity.FlinkColumnEntity;
import com.github.lessonone.fiflow.common.entity.FlinkDatabaseEntity;
import com.github.lessonone.fiflow.common.entity.FlinkTableEntity;
import com.github.lessonone.fiflow.common.utils.DbUtils;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 把数据库 数据表 字段信息 保存在 mysql 数据库中
 */
public class FlinkInDbCatalog extends AbstractCatalog {
    public static final String DEFAULT_DB = "default";
    private final DbInfo dbInfo;
    private MetaDbDao metaDbDao;
    // 参考
    private GenericInMemoryCatalog inMemoryCatalog;

    public FlinkInDbCatalog(String name, String defaultDatabase, DbInfo dbInfo) {
        super(name, defaultDatabase);
        this.dbInfo = dbInfo;
        this.metaDbDao = new MetaDbDao(DbUtils.createDatasource(dbInfo));
    }

    public FlinkInDbCatalog(String name, DbInfo dbInfo) {
        this(name, DEFAULT_DB, dbInfo);
    }

    @Override
    public void open() throws CatalogException {
    }

    @Override
    public void close() throws CatalogException {
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return metaDbDao.listDatabases(getName());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        Optional<FlinkDatabaseEntity> db = metaDbDao.getDatabase(getName(), databaseName);
        if (!db.isPresent())
            throw new DatabaseNotExistException(getName(), databaseName);
        return db.get().toCatalogDatabase();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        Optional<FlinkDatabaseEntity> db = metaDbDao.getDatabase(getName(), databaseName);
        return db.isPresent();
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        metaDbDao.createDatabase(getName(), name, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        metaDbDao.dropDatabase(getName(), name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        metaDbDao.alterDatabase(getName(), name, newDatabase, ignoreIfNotExists);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        return metaDbDao.listTables(getName(), databaseName);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        final Optional<FlinkTableEntity> tableEntity = metaDbDao.getTable(getName(), tablePath.getDatabaseName(), tablePath.getObjectName());
        if (!tableEntity.isPresent())
            throw new TableNotExistException(getName(), tablePath);

        final FlinkTableEntity tableInfo = tableEntity.get();

        // 字段信息 表结构
        List<FlinkColumnEntity> columns = metaDbDao.getColumns(tableInfo.getId());
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        if (columns != null) {
            for (FlinkColumnEntity column : columns) {
                DataType dataType = TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(column.getDataType()));
                if (column.getExpr() == null) {
                    schemaBuilder.add(TableColumn.of(column.getName(), dataType));
                } else {
                    schemaBuilder.add(TableColumn.of(column.getName(), dataType, column.getExpr()));
                }
            }
        }

        return new CatalogTableImpl(schemaBuilder.build(),
                tableInfo.getPartitionKeys(),
                tableInfo.getProperties(),
                tableInfo.getComment());
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return metaDbDao.tableExists(getName(), tablePath.getDatabaseName(), tablePath.getObjectName());
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        metaDbDao.dropTable(getName(), tablePath.getDatabaseName(), tablePath.getObjectName(), ignoreIfNotExists);
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists)
                throw new TableNotExistException(getName(), tablePath);
        }
        metaDbDao.renameTable(getName(), tablePath, newTableName);
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {


        metaDbDao.createTable(getName(), tablePath, table, ignoreIfExists);
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        metaDbDao.alterTable(getName(), tablePath, newTable, ignoreIfNotExists);
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
        return new ArrayList<>();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        // todo
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

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
