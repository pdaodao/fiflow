package com.github.myetl.fiflow.io.elasticsearch7;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

public class ESClient {
    private final TypeSerializer<Row> serializer;
    private final String indexName;

    private RestHighLevelClient client;

    private String scrollId = null;
    private volatile boolean running = true;


    public ESClient(String indexName, TypeSerializer<Row> serializer, String hosts, String username, String password) {
        this.indexName = indexName;
        this.serializer = serializer;
        this.client = EsUtils.createClient(hosts, username, password);
    }

    public void scrollSearch(RowTypeInfo rowTypeInfo, Integer shardIndex, SourceFunction.SourceContext<Row> ctx) throws IOException {
        if (running == false) return;
        scrollId = null;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchSourceBuilder.fetchSource(rowTypeInfo.getFieldNames(), null);
        // 使用 scroll api 获取数据
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(3l));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        searchRequest.scroll(scroll);
        searchRequest.source(searchSourceBuilder);

        if (shardIndex != null) {
            searchRequest.preference("_shards:" + shardIndex);
        }

        SearchHit[] hits = null;

        if (running == false) return;

        do {
            SearchResponse searchResponse;
            if (scrollId == null) {
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
            } else {
                searchResponse = client.scroll(new SearchScrollRequest(scrollId), RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
            }
            hits = searchResponse.getHits().getHits();
            if (hits != null) {
                for (SearchHit hit : hits) {
                    Map<String, Object> map = hit.getSourceAsMap();
                    Row row = serializer.createInstance();
                    int index = 0;
                    for (String f : rowTypeInfo.getFieldNames()) {
                        row.setField(index++, map.get(f));
                    }
                    ctx.collect(row);
                }
            }
        } while (running && hits != null && hits.length > 0);
        clearScroll();
    }


    private void clearScroll() {
        if (client == null) return;
        if (scrollId == null) return;
        try {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scrollId = null;
        }
    }


    public Integer shards() throws IOException {
        return EsUtils.shards(client, indexName);
    }

    public void cancel() {
        running = false;
    }


    public void close() {
        running = false;
        if (client == null)
            return;
        clearScroll();
        try {
            client.close();
        } catch (IOException e) {

        }
        client = null;
    }


}
