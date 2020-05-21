package com.github.lessonone.fiflow.core.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class TableResultSink implements AppendStreamTableSink<Row> {
    private final TableSchema schema;
    private final DataType rowType;
    private final RichSinkFunction<Row> sinkFunction;

    public TableResultSink(TableSchema schema, RichSinkFunction<Row> sinkFunction) {
        this.schema = schema;
        this.rowType = schema.toRowDataType();
        this.sinkFunction = sinkFunction;
    }

    @Override
    public DataType getConsumedDataType() {
        return rowType;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        throw new UnsupportedOperationException(
                "This sink is configured by passing a static schema when initiating");
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        throw new UnsupportedOperationException("Deprecated method, use consumeDataStream instead");
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(sinkFunction)
                .name("collect")
                .setParallelism(1);
    }
}