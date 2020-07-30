package com.github.lessonone.fiflow.common.entity;

/**
 * fi_flink_database
 */
public class FlinkDatabaseEntity {
    public static final String TableName = "fi_flink_database";
    public static final String SqlSelectByCatalog = "SELECT id, catalog, name, properties, comment FROM " + TableName + " WHERE catalog = ?";
    public static final String SqlDeleteByCatalogAndDatabase = "DELETE FROM " + TableName + " WHERE catalog = ? AND name = ?";

    private Long id;
    private String catalog;
    private String name;
    private String properties;
    private String comment;

    public Long getId() {
        return id;
    }

    public FlinkDatabaseEntity setId(Long id) {
        this.id = id;
        return this;
    }

    public String getCatalog() {
        return catalog;
    }

    public FlinkDatabaseEntity setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getName() {
        return name;
    }

    public FlinkDatabaseEntity setName(String name) {
        this.name = name;
        return this;
    }

    public String getProperties() {
        return properties;
    }

    public FlinkDatabaseEntity setProperties(String properties) {
        this.properties = properties;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public FlinkDatabaseEntity setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
