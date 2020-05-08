package com.github.myetl.fiflow.io.elasticsearch7;

import com.github.myetl.fiflow.core.io.ExpressionUtils;
import com.github.myetl.fiflow.core.io.TypeUtils;
import com.github.myetl.fiflow.io.elasticsearch7.core.ESOptions;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ESTableSource implements StreamTableSource<Row>, FilterableTableSource<Row>, LookupableTableSource<Row> {

    private final ESOptions esOptions;
    private final TableSchema schema;
    private final RowTypeInfo typeInfo;
    // 过滤条件部分
    private final String where;


    private ESTableSource(ESOptions esOptions, TableSchema schema, String where) {
        this.esOptions = esOptions;
        this.schema = schema;
        this.typeInfo = TypeUtils.toNormalizeRowType(schema);
        this.where = where;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return new EsLookTableFunction(esOptions, typeInfo, lookupKeys);
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return new EsAsyncLookTableFunction(esOptions, typeInfo, lookupKeys);
    }

    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(ESSourceFunction.builder()
                .setEsOptions(esOptions)
                .setRowTypeInfo(typeInfo)
                .setWhere(where)
                .build()).name(explainSource());
    }

    @Override
    public RowTypeInfo getReturnType() {
        return typeInfo;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String explainSource() {
        StringBuilder sb = new StringBuilder();
        String className = this.getClass().getSimpleName();
        String[] fields = typeInfo.getFieldNames();
        if (null == fields) {
            sb.append(className + "(*)");
        } else {
            sb.append(className + "(" + String.join(", ", fields) + ")");
        }
        sb.append(" from ").append(esOptions.getIndex());
        if (where != null) {
            sb.append(" where ").append(where);
        }
        return sb.toString();
    }

    @Override
    public TableSource<Row> applyPredicate(List<Expression> predicates) {
        String where = ExpressionUtils.toWhere(predicates, null);
        return new ESTableSource(esOptions, schema, where);
    }

    @Override
    public boolean isFilterPushedDown() {
        return where != null;
    }

    public static class Builder {
        private ESOptions esOptions;
        private TableSchema schema;
        private String where;

        public Builder setEsOptions(ESOptions esOptions) {
            this.esOptions = esOptions;
            return this;
        }

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setWhere(String where) {
            this.where = where;
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
            return new ESTableSource(esOptions, schema, where);
        }
    }
}