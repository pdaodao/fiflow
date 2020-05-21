package com.github.lessonone.fiflow.io.elasticsearch7.sink;

import com.github.lessonone.fiflow.core.io.TypeUtils;
import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import com.github.lessonone.fiflow.io.elasticsearch7.frame.ESClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ESSinkFunction<T> extends RichSinkFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ESSinkFunction.class);
    private ESOptions esOptions;
    private RowTypeInfo rowTypeInfo;

    private transient ESClient esClient;
    private transient BulkProcessor bulkProcessor;

    private transient Boolean isRow = null;

    protected ESSinkFunction() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        esClient = new ESClient(esOptions);

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {

            }
        };

        bulkProcessor = esClient.initBulk(listener);
    }

    public void append(Row row, Context context) throws Exception {
        IndexRequest indexRequest = new IndexRequest(esOptions.getIndex());
        Map<String, Object> map = new HashMap<>(rowTypeInfo.getArity());
        int index = 0;
        for (String field : rowTypeInfo.getFieldNames()) {
            map.put(field, row.getField(index++));
        }
        indexRequest.source(map);
        bulkProcessor.add(indexRequest);
    }

    public void delete(Row row, Context context) throws Exception {
        throw new UnsupportedOperationException("es delete doing");
    }

    protected void invokeTuple(Tuple2<Boolean, Row> value, Context context) throws Exception {
        System.out.println("-------" + value.toString());

        boolean isDelete = value.f0;
        Row row = value.f1;
        append(row, context);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (isRow == null) {
            isRow = value instanceof Row;
        }
        if (isRow) {
            append((Row) value, context);
        } else {
            invokeTuple((Tuple2<Boolean, Row>) value, context);
        }
    }

    @Override
    public void close() throws Exception {
        if (bulkProcessor != null) {
            bulkProcessor.flush();
            bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
            bulkProcessor = null;
        }
        if (esClient != null) {
            esClient.close();
            esClient = null;
        }
    }

    public static class Builder {
        private ESSinkFunction sinkFunction;

        public Builder() {
            this.sinkFunction = new ESSinkFunction();
        }

        public Builder setEsOptions(ESOptions esOptions) {
            this.sinkFunction.esOptions = esOptions;
            return this;
        }

        public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.sinkFunction.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public Builder setRowTypeInfo(TableSchema schema) {
            this.sinkFunction.rowTypeInfo = TypeUtils.toNormalizeRowType(schema);
            return this;
        }

        public ESSinkFunction build() {
            checkNotNull(sinkFunction.esOptions, "No options supplied.");
            checkNotNull(sinkFunction.rowTypeInfo, "No rowTypeInfo supplied.");
            return sinkFunction;
        }
    }

}
