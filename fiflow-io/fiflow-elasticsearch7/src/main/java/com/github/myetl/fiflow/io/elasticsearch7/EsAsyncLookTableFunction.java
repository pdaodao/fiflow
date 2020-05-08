package com.github.myetl.fiflow.io.elasticsearch7;

import com.github.myetl.fiflow.io.elasticsearch7.core.ESOptions;
import com.github.myetl.fiflow.io.elasticsearch7.frame.ESClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class EsAsyncLookTableFunction extends AsyncTableFunction<Row> {
    private final ESOptions esOptions;
    private final RowTypeInfo typeInfo;
    private transient ESClient esClient;
    private String queryTemplate;

    public EsAsyncLookTableFunction(ESOptions esOptions, RowTypeInfo typeInfo, String[] lookupKeys) {
        this.esOptions = esOptions;
        this.typeInfo = typeInfo;
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(StringUtils.join(typeInfo.getFieldNames(), ","));
        sb.append(" FROM ");
        sb.append("`").append(esOptions.getIndex()).append("`");
        sb.append(" WHERE ");
        sb.append(StringUtils.join(lookupKeys, " = ? "));
        sb.append(" = ? ");
        queryTemplate = sb.toString();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        esClient = new ESClient(esOptions);
    }

    public void eval(final CompletableFuture<Collection<Row>> result, Object... keys) {

        String sql = queryTemplate;
        for (Object k : keys) {
            sql = sql.replaceFirst("\\?", "`" + Objects.toString(k) + "`");
        }
        try {
            esClient.search(sql, new Collector<Row>() {
                final List<Row> rows = new ArrayList<>();

                @Override
                public void collect(Row record) {
                    rows.add(record);
                }

                @Override
                public void close() {
                    result.complete(rows);
                }
            }, true);
        } catch (Exception e) {
            result.completeExceptionally(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (esClient != null) {
            esClient.close();
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return typeInfo;
    }
}
