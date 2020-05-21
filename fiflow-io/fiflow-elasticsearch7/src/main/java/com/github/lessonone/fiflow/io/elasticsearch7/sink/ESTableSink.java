package com.github.lessonone.fiflow.io.elasticsearch7.sink;


import com.github.lessonone.fiflow.core.io.TypeUtils;
import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import com.github.lessonone.fiflow.io.elasticsearch7.core.ESTableBaseBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ESTableSink implements RetractStreamTableSink<Row> {
    private final ESOptions esOptions;
    private final TableSchema schema;
    private final RowTypeInfo typeInfo;

    private ESTableSink(ESOptions esOptions, TableSchema schema) {
        this.esOptions = esOptions;
        this.schema = schema;
        this.typeInfo = TypeUtils.toNormalizeRowType(schema);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return typeInfo;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        ESSinkFunction<Tuple2<Boolean, Row>> sinkFun = ESSinkFunction.builder()
                .setEsOptions(esOptions)
                .setRowTypeInfo(typeInfo)
                .build();
        return dataStream.addSink(sinkFun)
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames()));
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }

    public static class Builder extends ESTableBaseBuilder {


        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JDBCTableSource
         */
        public ESTableSink build() {
            checkNotNull(esOptions, "No EsOptions supplied.");
            checkNotNull(schema, "No schema supplied.");
            return new ESTableSink(esOptions, schema);
        }
    }

}
