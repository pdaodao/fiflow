package com.github.lessonone.fiflow.common.catalog;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * fi_flink_database
 */
public class FlinkCatalogDatabase extends CatalogDatabaseImpl {
    private final Long id;

    private String catalog;

    public FlinkCatalogDatabase(@Nullable Long id, Map<String, String> properties, @Nullable String comment) {
        super(properties, comment);
        this.id = id;
    }

    public FlinkCatalogDatabase setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getCatalog() {
        return catalog;
    }

    public Long getId() {
        return id;
    }

    @Override
    public CatalogDatabase copy() {
        return new FlinkCatalogDatabase(this.id, new HashMap<>(getProperties()), getComment());
    }
}
