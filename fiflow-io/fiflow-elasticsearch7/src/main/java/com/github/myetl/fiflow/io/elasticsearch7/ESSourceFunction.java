package com.github.myetl.fiflow.io.elasticsearch7;

import com.github.myetl.fiflow.core.io.IOSourceFunction;
import com.github.myetl.fiflow.core.io.TypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ESSourceFunction extends IOSourceFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ESSourceFunction.class);

    private String hosts;
    private String indexName;

    private String username;
    private String password;

    private RowTypeInfo rowTypeInfo;
    private String queryTemplate;

    private transient ESClient esClient;
    private volatile boolean isRunning = true;
    private transient TypeSerializer<Row> serializer;

    public ESSourceFunction() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        serializer = rowTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        Integer subIndex = getRuntimeContext().getIndexOfThisSubtask();
        Integer tasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.esClient = new ESClient(indexName, serializer, hosts, username, password);

        Integer shards = esClient.shards();
        List<Integer> subShards = new ArrayList<>();
        for (int i = 0; i < shards; i++) {
            if (i % tasks == subIndex) {
                subShards.add(i);
            }
        }
        for (Integer shard : subShards) {
            if (isRunning == false) break;
            LOG.info("elasticsearch scroll search {} : shard_{}", indexName, shard);
            esClient.scrollSearch(rowTypeInfo, shard, ctx);
        }
        esClient.close();
    }

    @Override
    public void cancel() {
        esClient.cancel();
        isRunning = false;

        if (esClient != null) {
            esClient.close();
            esClient = null;
        }
    }

    @Override
    public RowTypeInfo getProducedType() {
        return rowTypeInfo;
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    public static class Builder {
        private ESOptions esOptions;
        private ESReadOptions esReadOptions;
        private RowTypeInfo rowTypeInfo;
        private String queryTemplate;


        public Builder setEsOptions(ESOptions esOptions) {
            this.esOptions = esOptions;
            return this;
        }

        public Builder setEsReadOptions(ESReadOptions esReadOptions) {
            this.esReadOptions = esReadOptions;
            return this;
        }

        public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public Builder setRowTypeInfo(TableSchema schema) {
            this.rowTypeInfo = TypeUtils.toNormalizeRowType(schema);
            return this;
        }

        public Builder setQueryTemplate(String queryTemplate) {
            this.queryTemplate = queryTemplate;
            return this;
        }

        public ESSourceFunction build() {
            checkNotNull(esOptions, "No options supplied.");
            checkNotNull(rowTypeInfo, "No rowTypeInfo supplied.");
            if (esReadOptions == null) {
                esReadOptions = ESReadOptions.builder().build();
            }
            if (queryTemplate == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT ");
                sb.append(StringUtils.join(rowTypeInfo.getFieldNames(), ","));
                sb.append(" FROM ");
                sb.append(esOptions.getIndex());

                queryTemplate = sb.toString();
            }

            ESSourceFunction esSourceFunction = new ESSourceFunction();
            esSourceFunction.hosts = esOptions.getHosts();
            esSourceFunction.indexName = esOptions.getIndex();
            esSourceFunction.username = esOptions.getUsername();
            esSourceFunction.password = esOptions.getPassword();
            esSourceFunction.rowTypeInfo = rowTypeInfo;

            return esSourceFunction;
        }
    }


}
