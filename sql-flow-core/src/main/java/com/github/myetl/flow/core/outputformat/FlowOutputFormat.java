package com.github.myetl.flow.core.outputformat;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

/**
 * 输出
 */
public abstract class FlowOutputFormat extends RichOutputFormat<Row>
        implements ResultTypeQueryable<Row> {

    protected RowTypeInfo rowTypeInfo;


    public FlowOutputFormat(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    public void setRowTypeInfo(RowTypeInfo rowTypeInfo) {
        if (rowTypeInfo == null) {
            throw new IllegalArgumentException("FlowOutputFormat rowTypeInfo should be given by manu.");
        }
        this.rowTypeInfo = rowTypeInfo;
    }


    @Override
    public RowTypeInfo getProducedType() {
        return rowTypeInfo;
    }

    @Override
    public void configure(Configuration parameters) {

    }

}