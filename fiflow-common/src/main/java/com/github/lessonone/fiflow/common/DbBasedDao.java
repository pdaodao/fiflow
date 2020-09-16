package com.github.lessonone.fiflow.common;

import com.github.lessonone.fiflow.common.base.BaseDao;
import com.github.lessonone.fiflow.common.base.SqlWrap;
import com.github.lessonone.fiflow.common.entity.*;
import com.github.lessonone.fiflow.common.utils.StrUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.util.StringUtils;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

public class DbBasedDao extends BaseDao {
    public static final String Connector = "connector";

    public static Cache<String, FlinkConnectorType> connectorTypeCache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(200)
            .build();


    public DbBasedDao(DataSource ds) {
        super(ds);
    }

    public List<FlinkConnectorType> connectorTypeList() {
        List<FlinkConnectorType> types = queryForList(SqlWrap.builder().select("*").from(FlinkConnectorType.class).build(),
                FlinkConnectorType.class);
        final Map<String, FlinkConnectorType> map = new HashMap<>();
        types.forEach(t -> {
            map.put(t.getName().toLowerCase(), t);
        });
        return types.stream().map(t -> {
            if (t.getPname() != null && map.containsKey(t.getPname().toLowerCase())) {
                t = t.merge(map.get(t.getName().toLowerCase()));
            }
            return t;
        }).collect(Collectors.toList());
    }

    public Optional<FlinkConnector> getConnectorById(Long id) {
        return queryForOne(SqlWrap.builder().select("*").from(FlinkConnector.class).where("id").equal(id).build());
    }

    public Optional<FlinkConnectorType> getConnectorType(String connector) {
        connector = connector.toLowerCase();
        FlinkConnectorType connectorType = connectorTypeCache.getIfPresent(connector);
        if (connectorType != null) return Optional.of(connectorType);
        List<FlinkConnectorType> types = connectorTypeList();
        types.stream().forEach(t -> {
            connectorTypeCache.put(t.getName().toLowerCase(), t);
        });
        return Optional.ofNullable(connectorTypeCache.getIfPresent(connector));
    }

    public List<String> listCatalogs() {
        SqlWrap sql = SqlWrap.builder()
                .select("distinct catalog")
                .from(FlinkDatabaseEntity.class)
                .build();

        return queryForList(sql, String.class);
    }

    public List<String> listDatabases(String catalog) {
        SqlWrap sql = SqlWrap.builder()
                .select("name")
                .from(FlinkDatabaseEntity.class)
                .where("catalog").equal(catalog)
                .build();
        return queryForList(sql, String.class);
    }

    public Optional<FlinkDatabaseEntity> getDatabase(String catalog, String databaseName) {
        SqlWrap sql = SqlWrap.builder()
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
                SqlWrap.builder().count(FlinkTableEntity.class)
                        .where("database_id").equal(dbId)
                        .build(), Long.class);

        if (count.isPresent() && count.get() > 0) {
            if (!cascade)
                throw new DatabaseNotEmptyException(catalog, databaseName);
        }
        transactionWrap(() -> {
            final SqlWrap deleteColumnSql = SqlWrap.builder()
                    .delete(FlinkColumnEntity.class)
                    .where("table_id").in(SqlWrap
                            .builder()
                            .select("id").from(FlinkTableEntity.class)
                            .where("database_id").equal(dbId)
                            .build()
                    ).build();
            final SqlWrap deleteTableSql = SqlWrap.builder()
                    .delete(FlinkTableEntity.class)
                    .where("database_id").equal(dbId)
                    .build();
            final SqlWrap deleteDb = SqlWrap.builder().delete(FlinkDatabaseEntity.class).build();

            update(deleteColumnSql);
            update(deleteTableSql);
            update(deleteDb);
            return true;
        });
    }

    public void renameTable(final String catalog, final ObjectPath tablePath, final String newTableName) {
        Optional<FlinkTableEntity> tableInfo = getTable(catalog, tablePath.getDatabaseName(), tablePath.getObjectName());
        if (tableInfo.isPresent()) {
            FlinkTableEntity info = tableInfo.get();
            if (!info.getName().equals(newTableName)) {
                info.setName(newTableName);
                update(info);
            }
        }
    }

    public void dropTable(String catalog, String databaseName, String tableName, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        Optional<FlinkTableEntity> table = getTable(catalog, databaseName, tableName);
        if (!table.isPresent()) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(catalog, new ObjectPath(databaseName, tableName));
        }
        final Long tableId = table.get().getId();
        transactionWrap(() -> {
            SqlWrap deleteColumnSql = SqlWrap.builder()
                    .delete(FlinkColumnEntity.class)
                    .where("table_id").equal(tableId)
                    .build();
            SqlWrap deleteTableSql = SqlWrap.builder().delete(FlinkTableEntity.class)
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
        SqlWrap sqlSelect = SqlWrap.builder()
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

    public void alterTable(String catalog, ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {

    }


    /**
     * 创建或者获取一个已有的 connector
     *
     * @param tableEntity
     * @param connectorName
     */
    private void getOrCreateConnector(final FlinkTableEntity tableEntity, final String connectorName) {
        if (tableEntity.getProperties() == null) return;
        String connector = tableEntity.getProperties().get(Connector);
        if (connector == null) return;
        Optional<FlinkConnectorType> typeOptional = getConnectorType(connector);
        if (!typeOptional.isPresent()) return;
        FlinkConnectorType type = typeOptional.get();
        if (type.getOptions() == null || type.getOptions().size() < 1) return;

        tableEntity.getProperties().remove(Connector);
        if (type.getObjectKey() != null) {
            String objectName = tableEntity.getProperties().get(type.getObjectKey());
            if (objectName != null) {
                tableEntity.setObjectName(objectName);
                tableEntity.getProperties().remove(type.getObjectKey());
            }
        }

        Map<String, String> connectorProperties = new LinkedHashMap<>();
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, FlinkConnectorType.OptionDescriptor> entry : type.getOptions().entrySet()) {
            String key = entry.getKey();
            String value = tableEntity.getProperties().get(key);
            if (value != null) {
                connectorProperties.put(key, value);
                tableEntity.getProperties().remove(key);
                sb.append(key).append(":").append("value");
            } else if (entry.getValue().isRequired()) {
                throw new IllegalArgumentException("create table " + tableEntity.getName() + "property:" + key + " is required");
            }
        }

        final String hashCode = StrUtil.sha256(sb.toString());

        Optional<FlinkConnector> realConnector = queryForOne(SqlWrap.builder().select("id").from(FlinkConnector.class)
                .where("hash_code").equal(hashCode)
                .build());
        if (realConnector.isPresent()) {
            tableEntity.setConnectorId(realConnector.get().getId());
            return;
        }
        FlinkConnector toAddConnector = new FlinkConnector();
        toAddConnector.setName(connectorName);
        toAddConnector.setTypeName(connector);
        toAddConnector.setHashCode(hashCode);
        toAddConnector.setOptions(connectorProperties);

        Long connectorId = insert(toAddConnector);
        tableEntity.setConnectorId(connectorId);
    }


    public boolean createTable(String catalog, ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        Long dbId = getOrCreateDatabase(catalog, tablePath.getDatabaseName());
        Optional<FlinkTableEntity> oldTable = getTable(catalog, tablePath.getDatabaseName(), tablePath.getObjectName());


        FlinkTableEntity tableInfo = new FlinkTableEntity();
        tableInfo.setDatabaseId(dbId);
        tableInfo.setName(tablePath.getObjectName());
        tableInfo.setProperties(table.getOptions());
        tableInfo.setComment(table.getComment());

        // 获取 或者 创建 物理连接
        getOrCreateConnector(tableInfo, catalog + ":" + tablePath.getDatabaseName());


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
            int i = 1;
            for (TableColumn column : table.getSchema().getTableColumns()) {
                String dataType = column.getType().toString();
                FlinkColumnEntity field = new FlinkColumnEntity();
                field.setTableId(tableId);
                field.setName(column.getName());
                field.setDataType(dataType);
                field.setExpr(column.getExpr().orElse(null));
                field.setPosition(i++);
                insert(field);
            }
            return true;
        });

        return true;
    }

    public boolean tableExists(String catalog, String databaseName, String tableName) {
        SqlWrap sql = SqlWrap.builder()
                .select("t.id")
                .from(FlinkDatabaseEntity.class, "d")
                .innerJoin(FlinkTableEntity.class, "t")
                .on("d.id = t.database_id")
                .where("d.catalog").equal(catalog)
                .and("d.name").equal(databaseName)
                .and("t.name").equal(tableName)
                .build();
        Optional<Long> id = queryForOne(sql, Long.class);
        return id.isPresent();
    }

    public Optional<FlinkTableEntity> getTable(final String catalog, final String databaseName, final String tableName) {
        SqlWrap sql = SqlWrap.builder()
                .select("t.*")
                .from(FlinkDatabaseEntity.class, "d")
                .innerJoin(FlinkTableEntity.class, "t")
                .on("d.id = t.database_id")
                .where("d.catalog").equal(catalog)
                .and("d.name").equal(databaseName)
                .and("t.name").equal(tableName)
                .build();
        Optional<FlinkTableEntity> table = queryForOne(sql, FlinkTableEntity.class);
        if (table.isPresent() && table.get().getConnectorId() != null) {
            Optional<FlinkConnector> connectorOptional = getConnectorById(table.get().getConnectorId());
            if (connectorOptional.isPresent()) {
                FlinkConnector connector = connectorOptional.get();
                Map<String, String> props = connector.mergeTableProperties(table.get().getProperties());

                Optional<FlinkConnectorType> connectorType = getConnectorType(connector.getTypeName());
                if (connectorType.isPresent()) {
                    props.put(Connector, connectorType.get().getConnector());
                    if (connectorType.get().getObjectKey() != null && table.get().getObjectName() != null) {
                        props.put(connectorType.get().getObjectKey(), table.get().getObjectName());
                    }
                }
                table.get().setProperties(props);
            }
        }
        return table;
    }

    public List<FlinkColumnEntity> getColumns(Long tableId) {
        SqlWrap sql = SqlWrap.builder()
                .select("*").from(FlinkColumnEntity.class)
                .where("table_id").equal(tableId)
                .orderBy("position asc")
                .build();
        return queryForList(sql);
    }
}
