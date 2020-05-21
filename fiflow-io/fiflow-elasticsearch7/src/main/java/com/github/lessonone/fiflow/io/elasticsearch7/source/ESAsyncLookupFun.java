package com.github.lessonone.fiflow.io.elasticsearch7.source;

import com.github.lessonone.fiflow.core.io.RowCollector;
import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import com.github.lessonone.fiflow.io.elasticsearch7.frame.ESClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ESAsyncLookupFun extends AsyncTableFunction<Row> {
    private final ESOptions esOptions;
    private final RowTypeInfo typeInfo;
    private transient ESClient esClient;
    private String queryTemplate;

    public ESAsyncLookupFun(ESOptions esOptions, RowTypeInfo typeInfo, String[] lookupKeys) {
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
            sql = sql.replaceFirst("\\?", "`" + k + "`");
        }
        try {
            esClient.search(sql, true, new RowCollector() {
                final List<Row> rows = new ArrayList<>();

                @Override
                public void collect(Row record) {
                    rows.add(record);
                }

                @Override
                public void complete() {
                    result.complete(rows);
                }

                @Override
                public void error(Exception e) {
                    result.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            result.completeExceptionally(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (esClient != null) {
            esClient.close();
            esClient = null;
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return typeInfo;
    }
}
