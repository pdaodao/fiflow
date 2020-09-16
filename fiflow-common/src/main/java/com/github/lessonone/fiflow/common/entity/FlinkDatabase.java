package com.github.lessonone.fiflow.common.entity;

import com.github.lessonone.fiflow.common.base.Table;
import lombok.Data;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * flink catalog 中的 数据库概念
 */
@Data
@Table("fi_flink_database")
public class FlinkDatabase extends BaseEntity {
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
