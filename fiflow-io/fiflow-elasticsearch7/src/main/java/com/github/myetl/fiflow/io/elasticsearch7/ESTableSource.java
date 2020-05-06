package com.github.myetl.fiflow.io.elasticsearch7;

import com.github.myetl.fiflow.core.io.TypeUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ESTableSource implements StreamTableSource<Row>, FilterableTableSource<Row> {

    private final ESOptions esOptions;
    private final TableSchema schema;
    private final RowTypeInfo returnType;


    private ESTableSource(ESOptions esOptions, TableSchema schema) {
        this.esOptions = esOptions;
        this.schema = schema;
        this.returnType = TypeUtils.toNormalizeRowType(schema);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(ESSourceFunction.builder()
                .setEsOptions(esOptions)
                .setRowTypeInfo(returnType)
                .build()).name(explainSource());
    }

    @Override
    public RowTypeInfo getReturnType() {
        return returnType;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String explainSource() {
        return TableConnectorUtils.generateRuntimeName(getClass(), returnType.getFieldNames());
    }

    @Override
    public TableSource<Row> applyPredicate(List<Expression> predicates) {

        System.out.println(predicates.toString());


        return this;
    }

    @Override
    public boolean isFilterPushedDown() {
        return false;
    }

    public static class Builder {
        private ESOptions esOptions;
        private TableSchema schema;

        public Builder setEsOptions(ESOptions esOptions) {
            this.esOptions = esOptions;
            return this;
        }

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JDBCTableSource
         */
        public ESTableSource build() {
            checkNotNull(esOptions, "No EsOptions supplied.");
            checkNotNull(schema, "No schema supplied.");
            return new ESTableSource(esOptions, schema);
        }
    }
}