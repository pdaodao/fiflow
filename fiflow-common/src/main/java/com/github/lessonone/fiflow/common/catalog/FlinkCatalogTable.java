package com.github.lessonone.fiflow.common.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;

import java.util.List;
import java.util.Map;

public class FlinkCatalogTable extends CatalogTableImpl {
    private final Long id;
    private Long databaseId;

    public FlinkCatalogTable(Long id, TableSchema tableSchema, Map<String, String> properties, String comment) {
        super(tableSchema, properties, comment);
        this.id = id;
    }

    public FlinkCatalogTable(Long id, TableSchema tableSchema, List<String> partitionKeys, Map<String, String> properties, String comment) {
        super(tableSchema, partitionKeys, properties, comment);
        this.id = id;
    }

    public Long getDatabaseId() {
        return databaseId;
    }

    public FlinkCatalogTable setDatabaseId(Long databaseId) {
        this.databaseId = databaseId;
        return this;
    }
}
