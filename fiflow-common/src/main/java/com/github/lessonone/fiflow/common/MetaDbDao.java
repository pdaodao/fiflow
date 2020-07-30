package com.github.lessonone.fiflow.common;

import com.github.lessonone.fiflow.common.base.DbInfo;
import com.github.lessonone.fiflow.common.catalog.FlinkCatalogDatabase;
import com.github.lessonone.fiflow.common.catalog.FlinkCatalogTable;
import com.github.lessonone.fiflow.common.utils.DbUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;

public class MetaDbDao {
    private final DbInfo dbInfo;
    private transient JdbcTemplate jdbcTemplate;
    private transient TransactionTemplate transactionTemplate;

    public MetaDbDao(DbInfo dbInfo) {
        this.dbInfo = dbInfo;
    }

    public void open() throws CatalogException {
        jdbcTemplate = DbUtils.createJdbcTemplate(dbInfo);
        transactionTemplate = DbUtils.createTransactionTemplate(dbInfo);
    }

    public void close() throws CatalogException {
        DbUtils.closeDatasource(dbInfo);
    }

    public List<String> listCatalogs() {
        String sql = "SELECT distinct catalog FROM fi_flink_database";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    public List<String> listDatabases(String catalog) {
        String sql = "SELECT name FROM fi_flink_database where catalog = ?";
        return jdbcTemplate.queryForList(sql, String.class, catalog);
    }

    public FlinkCatalogDatabase getDatabase(String catalog, String databaseName) throws DatabaseNotExistException, CatalogException {
        String sql = "SELECT catalog , name , properties , comment from fi_flink_database where catalog = ? and name = ?";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, catalog, databaseName);
        if (CollectionUtils.isEmpty(rows)) throw new DatabaseNotExistException(catalog, databaseName, null);
        Map<String, Object> row = rows.get(0);

        Long id = DbUtils.getValueAsLong(row, "id");
        Map<String, String> properties = DbUtils.getValueAsMapString(row, "properties");

        String comment = DbUtils.getValueAsString(row, "comment");
        return new FlinkCatalogDatabase(id, properties, comment).setCatalog(catalog);
    }

    public void createDatabase(final String catalog, final String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        try {
            CatalogDatabase old = getDatabase(catalog, name);
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(catalog, name);
            }
        } catch (DatabaseNotExistException e) {
            Map<String, Object> row = new HashMap<>();
            row.put("catalog", catalog);
            row.put("name", name);
            row.put("properties", database.getProperties());
            row.put("comment", database.getComment());
            DbUtils.insertIntoIgnoreNull(jdbcTemplate, "fi_flink_database", row);
        }
    }

    public void dropDatabase(final String catalog, final String databaseName, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        FlinkCatalogDatabase db = getDatabase(catalog, databaseName);
        String countTableSql = "select count(1) from fi_flink_table where database_id = ?";
        Long count = jdbcTemplate.queryForObject(countTableSql, Long.class, db.getId());
        final String deleteDbSql = "delete from fi_flink_database where id = ?";

        if (count == 0) {
            jdbcTemplate.update(deleteDbSql, db.getId());
            return;
        }
        if (!cascade) {
            throw new DatabaseNotEmptyException(catalog, databaseName);
        }
        final String deleteColumnSql = "delete from fi_flink_table_column where table_id in (\n" +
                "\tselect id from fi_flink_table where database_id = ?\n" +
                ")";
        final String deleteTableSql = "delete from fi_flink_table where database_id = ?";
        transactionWrap(() -> {
            jdbcTemplate.update(deleteColumnSql, db.getId());
            jdbcTemplate.update(deleteTableSql, db.getId());
            jdbcTemplate.update(deleteDbSql, db.getId());
            return null;
        });
    }


    public void alterDatabase(final String catalog, final String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        FlinkCatalogDatabase db = getDatabase(catalog, databaseName);
        Map<String, Object> row = new HashMap<>();
        row.put("properties", newDatabase.getProperties());
        row.put("comment", newDatabase.getComment());
        DbUtils.updateIgnoreNullById(jdbcTemplate, "fi_flink_database", row, db.getId());
    }


    public List<String> listTables(final String catalog, final String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        String sql = "SELECT name from fi_flink_database where catalog = ? and name = ?";
        return jdbcTemplate.queryForList(sql, String.class, catalog, databaseName);
    }


    private Long getOrCreateDatabase(final String catalog, final String databaseName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        String sql = "SELECT id, catalog, name from fi_flink_database where catalog = ? and name = ?";
        List<Map<String, Object>> ret = jdbcTemplate.queryForList(sql, catalog, databaseName);
        if (CollectionUtils.isEmpty(ret)) {
            final String insert = "INSERT INTO fi_flink_database(catalog, name) VALUES (?, ?)";
            return DbUtils.insertReturnAutoId(jdbcTemplate, insert, catalog, databaseName);
        } else {
            return (Long) ret.get(0).get("id");
        }
    }

    private void transactionWrap(final Supplier f) {
        transactionTemplate.execute(new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(TransactionStatus transactionStatus) {
                return f.get();
            }
        });
    }

    public boolean createTable(String catalog, ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        Long dbId = getOrCreateDatabase(catalog, tablePath.getDatabaseName());
        String tableName = tablePath.getObjectName();
        String realTableName = table.getOptions().get("connector.table");
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("database_id", dbId);
        rowMap.put("name", tableName);
        rowMap.put("object_name", realTableName);
        rowMap.put("properties", table.getOptions());
        rowMap.put("comment", table.getComment());

        if (table instanceof CatalogTable) {
            CatalogTable catalogTable = (CatalogTable) table;
            rowMap.put("partitioned", catalogTable.isPartitioned());
            rowMap.put("partition_keys", catalogTable.getPartitionKeys());
        }

        if (table.getSchema().getPrimaryKey().isPresent()) {
            UniqueConstraint pk = table.getSchema().getPrimaryKey().get();
            rowMap.put("primary_key_name", pk.getName());
            rowMap.put("primary_key_columns", pk.getColumns());
            rowMap.put("primary_key_type", pk.getType().toString());
        }

        if (CollectionUtils.isNotEmpty(table.getSchema().getWatermarkSpecs())) {


        }

        // water todo
        table.getSchema();

        transactionWrap(() -> {
            Long tableId = DbUtils.insertReturnAutoId(jdbcTemplate, "fi_flink_table", rowMap);
            String sql = "INSERT INTO fi_flink_table_column(table_id, name, data_type, expr) values (?, ?,?, ?)";
            List<Object[]> values = new ArrayList<>();
            for (TableColumn column : table.getSchema().getTableColumns()) {
//                String dataType = column.getType().getLogicalType().asSerializableString();
                String dataType = column.getType().toString();
                values.add(new Object[]{tableId, column.getName(), dataType, column.getExpr().orElse(null)});
            }
            jdbcTemplate.batchUpdate(sql, values);
            return null;
        });
        return true;
    }


    public boolean tableExists(String catalog, ObjectPath tablePath) {
        String sql = "SELECT count(1) FROM fi_flink_database a " +
                "INNER JOIN fi_flink_table b ON b.database_id = a.id " +
                "WHERE a.catalog = ? AND a.name = ? AND b.name = ?";
        Long count = jdbcTemplate.queryForObject(sql, Long.class, catalog, tablePath.getDatabaseName(), tablePath.getObjectName());
        return count > 0;
    }

    public FlinkCatalogTable getTable(String catalog, ObjectPath tablePath) throws TableNotExistException {
        String tableSql = "SELECT b.* \n" +
                "FROM fi_flink_database a \n" +
                "LEFT JOIN fi_flink_table b ON b.database_id = a.id \n" +
                "WHERE a.catalog = ? AND a.name = ? AND b.name = ? ";

        String columnSql = "SELECT name, data_type, expr FROM fi_flink_table_column where table_id = ?";

        List<Map<String, Object>> tables = jdbcTemplate.queryForList(tableSql, catalog, tablePath.getDatabaseName(), tablePath.getObjectName());
        if (CollectionUtils.isEmpty(tables)) {
            throw new TableNotExistException(catalog, tablePath);
        }
        Map<String, Object> tableInfo = tables.get(0);
        Long tableId = DbUtils.getValueAsLong(tableInfo, "id");
        Map<String, String> properties = DbUtils.getValueAsMapString(tableInfo, "properties");
        String comment = DbUtils.getValueAsString(tableInfo, "comment");

        List<Map<String, Object>> columnMaps = jdbcTemplate.queryForList(columnSql, tableId);

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (Map<String, Object> row : columnMaps) {
            String name = DbUtils.getValueAsString(row, "name");
            String type = DbUtils.getValueAsString(row, "data_type");
            String expr = DbUtils.getValueAsString(row, "expr");
            DataType dataType = TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(type));
            if (expr == null) {
                schemaBuilder.add(TableColumn.of(name, dataType));
            } else {
                schemaBuilder.add(TableColumn.of(name, dataType, expr));
            }
        }

        return new FlinkCatalogTable(tableId, schemaBuilder.build(), properties, comment);
    }


}
