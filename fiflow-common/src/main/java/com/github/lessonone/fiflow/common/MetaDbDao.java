package com.github.lessonone.fiflow.common;

import com.github.lessonone.fiflow.common.base.BaseDao;
import com.github.lessonone.fiflow.common.base.SqlSelect;
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
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

public class MetaDbDao extends BaseDao {


    public MetaDbDao(DataSource ds) {
        super(ds);
    }

    public List<String> listCatalogs() {
        SqlSelect sql = SqlSelect.builder()
                .select("distinct catalog")
                .from(FlinkDatabaseEntity.class)
                .build();

        return queryForList(sql, String.class);
    }

    public List<String> listDatabases(String catalog) {
        SqlSelect sql = SqlSelect.builder()
                .select("name")
                .from(FlinkDatabaseEntity.class)
                .where("catalog").equal(catalog)
                .build();
        return queryForList(sql, String.class);
    }

    public Optional<FlinkDatabaseEntity> getDatabase(String catalog, String databaseName) {
        SqlSelect sql = SqlSelect.builder()
                .select("*")
                .from(FlinkDatabaseEntity.class)
                .whereSql("catalog = ? and name = ?", catalog, databaseName)
                .build();
        return queryForOne(sql, FlinkDatabaseEntity.class);
    }

    public void createDatabase(final String catalog, final String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        Optional<FlinkDatabaseEntity> dbInfo = getDatabase(catalog, name);
        if (dbInfo.isPresent()) {
            if (!ignoreIfExists)
                throw new DatabaseAlreadyExistException(catalog, name);
            return;
        }
        FlinkDatabaseEntity db = new FlinkDatabaseEntity();
        db.setCatalog(catalog);
        db.setName(name);
        db.setProperties(database.getProperties());
        db.setComment(database.getComment());
        insert(db);
    }

    public void dropDatabase(final String catalog, final String databaseName, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        Optional<FlinkDatabaseEntity> dbInfo = getDatabase(catalog, databaseName);
        if (!dbInfo.isPresent()) {
            if (!ignoreIfNotExists) throw new DatabaseNotExistException(catalog, databaseName);
            return;
        }
        final Long dbId = dbInfo.get().getId();

        Optional<Long> count = queryForOne(
                SqlSelect.builder().count(FlinkTableEntity.class)
                        .where("database_id").equal(dbId)
                        .build(), Long.class);

        if (count.isPresent() && count.get() > 0) {
            if (!cascade)
                throw new DatabaseNotEmptyException(catalog, databaseName);
        }
        transactionWrap(() -> {
            final SqlSelect deleteColumnSql = SqlSelect.builder()
                    .delete(FlinkColumnEntity.class)
                    .where("table_id").in(SqlSelect
                            .builder()
                            .select("id").from(FlinkTableEntity.class)
                            .where("database_id").equal(dbId)
                            .build()
                    ).build();
            final SqlSelect deleteTableSql = SqlSelect.builder()
                    .delete(FlinkTableEntity.class)
                    .where("database_id").equal(dbId)
                    .build();
            final SqlSelect deleteDb = SqlSelect.builder().delete(FlinkDatabaseEntity.class).build();

            update(deleteColumnSql);
            update(deleteTableSql);
            update(deleteDb);
            return true;
        });
    }


    public void dropTable(String catalog, String databaseName, String tableName, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        Optional<FlinkTableEntity> table = getTable(catalog, databaseName, tableName);
        if (!table.isPresent()) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(catalog, new ObjectPath(databaseName, tableName));
        }
        final Long tableId = table.get().getId();
        transactionWrap(() -> {
            SqlSelect deleteColumnSql = SqlSelect.builder()
                    .delete(FlinkColumnEntity.class)
                    .where("table_id").equal(tableId)
                    .build();
            SqlSelect deleteTableSql = SqlSelect.builder().delete(FlinkTableEntity.class)
                    .where("id").equal(tableId).build();
            update(deleteColumnSql);
            update(deleteTableSql);
            return true;
        });
    }


    public void alterDatabase(final String catalog, final String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        final Optional<FlinkDatabaseEntity> db = getDatabase(catalog, databaseName);
        if (!db.isPresent())
            throw new DatabaseNotExistException(catalog, databaseName);
        final FlinkDatabaseEntity dbInfo = db.get();
        dbInfo.setComment(newDatabase.getComment());
        dbInfo.setProperties(newDatabase.getProperties());

        update(dbInfo);
    }

    public List<String> listTables(final String catalog, final String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        SqlSelect sqlSelect = SqlSelect.builder()
                .select("a.name").from(FlinkTableEntity.class, "a")
                .innerJoin(FlinkDatabaseEntity.class, "b")
                .on("a.database_id = b.id")
                .where("b.catalog").equal(catalog)
                .and("b.name").equal(databaseName)
                .build();
        return queryForList(sqlSelect, String.class);
    }


    private Long getOrCreateDatabase(final String catalog, final String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        Optional<FlinkDatabaseEntity> dbInfo = getDatabase(catalog, databaseName);
        if (dbInfo.isPresent()) return dbInfo.get().getId();

        FlinkDatabaseEntity db = new FlinkDatabaseEntity();
        db.setCatalog(catalog);
        db.setName(databaseName);
        return insert(db);
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

        if (table instanceof CatalogTable) {
            CatalogTable catalogTable = (CatalogTable) table;
            tableInfo.setPartitioned(catalogTable.isPartitioned());
            tableInfo.setPartitionKeys(catalogTable.getPartitionKeys());
        }

        // todo
        if (CollectionUtils.isNotEmpty(table.getSchema().getWatermarkSpecs())) {


        }

        transactionWrap(() -> {
            final Long tableId = insert(tableInfo);
            for (TableColumn column : table.getSchema().getTableColumns()) {
                String dataType = column.getType().toString();
                FlinkColumnEntity field = new FlinkColumnEntity();
                field.setTableId(tableId);
                field.setName(column.getName());
                field.setDataType(dataType);
                field.setExpr(column.getExpr().orElse(null));
                insert(field);
            }
            return true;
        });

        return true;
    }

    public Optional<FlinkTableEntity> getTable(final String catalog, final String databaseName, final String tableName) {
//        SqlSelect sql = SqlSelect.builder()
//                .select("a.*").from(FlinkTableEntity.class, "a")
//                .innerJoin(FlinkDatabaseEntity.class, "b")
//                .on("a.database_id = b.id")
//                .where("b.catalog").equal(catalog)
//                .and("b.name").equal(databaseName)
//                .and("a.name").equal(tableName)
//                .build();


        SqlSelect sql = SqlSelect.builder()
                .select("t.id, t.database_id, t.name, t.properties")
                .from(FlinkDatabaseEntity.class, "d")
                .innerJoin(FlinkTableEntity.class, "t")
                .on("d.id = t.database_id")
                .where("d.catalog").equal(catalog)
                .and("d.name").equal(databaseName)
                .and("t.name").equal(tableName)
                .build();



        System.out.println(sql.getSql());

        return queryForOne(sql, FlinkTableEntity.class);
    }

    public List<FlinkColumnEntity> getColumns(Long tableId) {
        SqlSelect sql = SqlSelect.builder()
                .select("*").from(FlinkColumnEntity.class).where("table_id").equal(tableId)
                .build();
        return queryForList(sql, FlinkColumnEntity.class);
    }
}
