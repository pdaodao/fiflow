package com.github.lessonone.fiflow.common;

import com.github.lessonone.fiflow.common.base.BaseDao;
import com.github.lessonone.fiflow.common.entity.FlinkColumnEntity;
import com.github.lessonone.fiflow.common.entity.FlinkDatabaseEntity;
import com.github.lessonone.fiflow.common.entity.FlinkTableEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.util.StringUtils;
import javax.sql.DataSource;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;

public class MetaDbDao extends BaseDao {
    public static final String TableFlinkDatabase = "fi_flink_database";
    public static final String TableFlinkTable = "fi_flink_table";
    public static final String TableFlinkColumn = "fi_flink_table_column";

    public static final String TableFlinkConnector = "fi_flink_connector";
    private static final String TableFlinkConnectorType = "fi_flink_connector_type";


    public MetaDbDao(DataSource ds) {
        super(ds);
    }


    public List<String> listCatalogs() {
        String sql = "SELECT distinct catalog FROM "+ TableFlinkDatabase;
        return queryForList(sql, String.class);
    }

    public List<String> listDatabases(String catalog) {
        String sql = "SELECT name FROM "+ TableFlinkDatabase+" where catalog = ?";
        return queryForList(sql, String.class, catalog);
    }

    public Optional<FlinkDatabaseEntity> getDatabase(String catalog, String databaseName) {
        String sql = "SELECT * from "+ TableFlinkDatabase+" where catalog = ? and name = ?";
        FlinkDatabaseEntity entity = null;
        return queryForOne(sql, FlinkDatabaseEntity.class,  catalog, databaseName);
    }

    public void createDatabase(final String catalog, final String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        Optional<FlinkDatabaseEntity> dbInfo = getDatabase(catalog, name);
        if(dbInfo.isPresent()){
            if(!ignoreIfExists)
                throw new DatabaseAlreadyExistException(catalog, name);
            return;
        }

        FlinkDatabaseEntity db = new FlinkDatabaseEntity();
        db.setCatalog(catalog);
        db.setName(name);
        db.setProperties(database.getProperties());
        db.setComment(database.getComment());
        insertSelective(TableFlinkDatabase, db);
    }

    public void dropDatabase(final String catalog, final String databaseName, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        Optional<FlinkDatabaseEntity> dbInfo = getDatabase(catalog, databaseName);
        if(!dbInfo.isPresent()){
            if(!ignoreIfNotExists) throw new DatabaseNotExistException(catalog, databaseName);
            return;
        }
        final Long dbId = dbInfo.get().getId();
        Optional<Long> count = queryForOne("SELECT count(1) FROM "+TableFlinkTable+" WHERE database_id = ?", Long.class, dbInfo.get().getId());
        if(count.isPresent() && count.get() > 0){
            if(!cascade)
                throw new DatabaseNotEmptyException(catalog, databaseName);;
        }
        transactionWrap(() -> {
            final String deleteColumnSql = "DELETE FROM "+TableFlinkColumn+" WHERE table_id in ("
                                          + "SELECT id FROM "+TableFlinkTable+" WHERE database_id = ?"
                                            +")";
            final String deleteTableSql = "delete from "+TableFlinkTable+" where database_id = ?";
            final String deleteDb = "DELETE FROM "+TableFlinkDatabase+" WHERE id = ?";
            update(deleteColumnSql, dbId);
            update(deleteTableSql, dbId);
            update(deleteDb, dbId);
            return true;
        });
    }

    public void dropTable(String catalog, ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException{
        Optional<FlinkTableEntity> table = getTable(catalog, tablePath);
        if(!table.isPresent()){
            if(ignoreIfNotExists) return;
            throw new TableNotExistException(catalog, tablePath);
        }
        final Long tableId = table.get().getId();
        transactionWrap(() -> {
            String deleteColumnSql = "DELETE FROM "+TableFlinkColumn+" WHERE table_id = ?";
            String deleteTableSql = "DELETE FROM "+TableFlinkTable+ " WHERE id = ?";
            update(deleteColumnSql, tableId);
            update(deleteTableSql, tableId);
            return true;
        });
    }


    public void alterDatabase(final String catalog, final String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        final Optional<FlinkDatabaseEntity> db = getDatabase(catalog, databaseName);
        if(!db.isPresent())
            throw new DatabaseNotExistException(catalog, databaseName);
        final FlinkDatabaseEntity dbInfo = db.get();
        dbInfo.setComment(newDatabase.getComment());
        dbInfo.setProperties(newDatabase.getProperties());
        updateSelective(TableFlinkDatabase, dbInfo);
    }

    public List<String> listTables(final String catalog, final String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        String sql = "SELECT a.name FROM "+TableFlinkTable+" a  INNER JOIN "+TableFlinkDatabase+" b "
                +" ON a.database_id = b.id WHERE b.catalog = ? AND a.name = ?";
        return queryForList(sql, String.class, catalog, databaseName);
    }


    private Long getOrCreateDatabase(final String catalog, final String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        Optional<FlinkDatabaseEntity> dbInfo = getDatabase(catalog, databaseName);
        if(dbInfo.isPresent()) return dbInfo.get().getId();

        FlinkDatabaseEntity db = new FlinkDatabaseEntity();
        db.setCatalog(catalog);
        db.setName(databaseName);
        return insertSelective(TableFlinkDatabase, db);
    }

    public boolean createTable(String catalog, ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        Long dbId = getOrCreateDatabase(catalog, tablePath.getDatabaseName());

        FlinkTableEntity tableInfo = new FlinkTableEntity();
        tableInfo.setDatabaseId(dbId);
        tableInfo.setName(tablePath.getObjectName());
        tableInfo.setProperties(table.getOptions());
        tableInfo.setComment(table.getComment());

        // todo
//        tableInfo.setObjectName();

        if(table instanceof CatalogTable){
            CatalogTable catalogTable = (CatalogTable) table;
            tableInfo.setPartitioned(catalogTable.isPartitioned());
            tableInfo.setPartitionKeys(catalogTable.getPartitionKeys());
        }

        // todo
        if (CollectionUtils.isNotEmpty(table.getSchema().getWatermarkSpecs())) {


        }

        transactionWrap(() -> {
            final Long tableId = insertSelective(TableFlinkTable, tableInfo);
            for(TableColumn column: table.getSchema().getTableColumns()){
                String dataType = column.getType().toString();
                FlinkColumnEntity field = new FlinkColumnEntity();
                field.setTableId(tableId);
                field.setName(column.getName());
                field.setDataType(dataType);
                field.setExpr(column.getExpr().orElse(null));
                insertSelective(TableFlinkColumn, field);
            }
            return true;
        });

        return true;
    }

    public Optional<FlinkTableEntity> getTable(String catalog, ObjectPath tablePath) {
        String sql = "SELECT a.* FROM "+TableFlinkTable+" a INNER JOIN "+TableFlinkDatabase+" b"
                +" ON a.database_id = b.id WHERE b.catalog = ? AND b.name = ? and a.name = ?";
        return queryForOne(sql, FlinkTableEntity.class, catalog, tablePath.getDatabaseName(), tablePath.getObjectName());
    }

    public List<FlinkColumnEntity> getColumns(Long tableId) {
        String sql = "SELECT * FROM "+TableFlinkColumn+" WHERE table_id = ?";
        return queryForList(sql, FlinkColumnEntity.class, tableId);
    }
}
