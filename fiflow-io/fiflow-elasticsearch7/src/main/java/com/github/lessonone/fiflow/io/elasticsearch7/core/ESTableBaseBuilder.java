package com.github.lessonone.fiflow.io.elasticsearch7.core;

import org.apache.flink.table.api.TableSchema;


public abstract class ESTableBaseBuilder {
    protected ESOptions esOptions;
    protected TableSchema schema;

    public ESTableBaseBuilder setEsOptions(ESOptions esOptions) {
        this.esOptions = esOptions;
        return this;
    }

    public ESTableBaseBuilder setSchema(TableSchema schema) {
        this.schema = schema;
        return this;
    }

    public abstract <T> T build();


}
