package com.github.myetl.fiflow.io.elasticsearch7;

import com.github.myetl.fiflow.io.elasticsearch7.core.ESOptions;
import com.github.myetl.fiflow.io.elasticsearch7.frame.ESClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Objects;

public class EsLookTableFunction extends TableFunction<Row> {
    private final ESOptions esOptions;
    private final RowTypeInfo typeInfo;
    private transient ESClient esClient;
    private String queryTemplate;

    public EsLookTableFunction(ESOptions esOptions, RowTypeInfo typeInfo, String[] lookupKeys) {
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

    public void eval(Object... keys) {
        String sql = queryTemplate;
        for (Object k : keys) {
            sql = sql.replaceFirst("\\?", "`" + Objects.toString(k) + "`");
        }
        try {
            esClient.search(sql, collector, false);
        } catch (Exception e) {
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
