package com.github.myetl.fiflow.io.elasticsearch7.frame;

import com.github.myetl.fiflow.io.elasticsearch7.core.ESOptions;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESClient {
    private final TypeSerializer<Row> serializer;
    private final ESOptions esOptions;

    private RestHighLevelClient client;

    private String scrollId = null;
    private volatile boolean running = true;


    public ESClient(ESOptions esOptions, TypeSerializer<Row> serializer) {
        this.esOptions = esOptions;
        this.serializer = serializer;
        this.client = EsUtils.createClient(esOptions.getHosts(), esOptions.getUsername(), esOptions.getPassword());
    }

    public void scrollSearch(String queryTemplate, Integer shardIndex, SourceFunction.SourceContext<Row> ctx) throws Exception {
        if (running == false) return;
        scrollId = null;

        Tuple2<SearchRequest, List<String>> build = EsBuilder.build(queryTemplate);
        SearchRequest searchRequest = build.f0;
        List<String> selectFields = build.f1;

        // 使用 scroll api 获取数据
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(3l));
        searchRequest.scroll(scroll);

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
                    for (String f : selectFields) {
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
        return EsUtils.shards(client, esOptions.getIndex());
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
