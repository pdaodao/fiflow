package com.github.lessonone.fiflow.common.entity;

import lombok.Data;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * fi_flink_database
 */
@Data
public class FlinkDatabaseEntity extends BaseEntity {
    private String catalog;
    private String name;
    private Map<String, String> properties;
    private String comment;

    public CatalogDatabase toCatalogDatabase() {
        if (properties == null) properties = new HashMap<>();
        CatalogDatabaseImpl db = new CatalogDatabaseImpl(properties, comment);
        return db;
    }
}
