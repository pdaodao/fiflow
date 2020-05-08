package com.github.myetl.fiflow.io.elasticsearch7.frame;

import com.github.myetl.fiflow.io.elasticsearch7.core.ESOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.ActionListener;
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
    private final ESOptions esOptions;
    private RestHighLevelClient client;
    private String scrollId = null;
    private volatile boolean running = true;

    public ESClient(ESOptions esOptions) {
        this.esOptions = esOptions;
        this.client = EsUtils.createClient(esOptions.getHosts(), esOptions.getUsername(), esOptions.getPassword());
    }

    public void search(String queryTemplate, final Collector<Row> collector, boolean isAsync) throws Exception {
        Tuple2<SearchRequest, List<String>> build = EsPushBuilder.build(queryTemplate);
        SearchRequest searchRequest = build.f0;
        List<String> selectFields = build.f1;
        searchRequest.source().size(10000);
        if (isAsync) {
            final CollectListener listener = new CollectListener(collector, selectFields);
            client.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);
            return;
        }
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        processResponse(response, collector, selectFields);
    }

    private int processResponse(SearchResponse response, Collector<Row> collector, List<String> selectFields) {
        if (response == null || response.getHits() == null) return 0;
        SearchHit[] hits = response.getHits().getHits();
        if (hits == null || hits.length < 1) return 0;
        int size = 0;
        for (SearchHit hit : hits) {
            Map<String, Object> map = hit.getSourceAsMap();
            Row row = new Row(selectFields.size());
            int index = 0;
            for (String f : selectFields) {
                row.setField(index++, map.get(f));
            }
            size++;
            collector.collect(row);
        }
        return size;
    }

    public void scrollSearch(String queryTemplate, Integer shardIndex, Collector<Row> collector) throws Exception {
        if (running == false) return;
        scrollId = null;

        System.out.println("---  es scroll search --- shard_" + shardIndex + ":" + queryTemplate);

        Tuple2<SearchRequest, List<String>> build = EsPushBuilder.build(queryTemplate);
        SearchRequest searchRequest = build.f0;
        List<String> selectFields = build.f1;

        // 使用 scroll api 获取数据
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(3l));
        searchRequest.scroll(scroll);

        if (shardIndex != null) {
            searchRequest.preference("_shards:" + shardIndex);
        }

        if (running == false) return;
        int bulkSize = 0;
        do {
            SearchResponse searchResponse;
            if (scrollId == null) {
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
            } else {
                searchResponse = client.scroll(new SearchScrollRequest(scrollId), RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
            }

            bulkSize = processResponse(searchResponse, collector, selectFields);

        } while (running && bulkSize > 0);
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

    public final class CollectListener implements ActionListener<SearchResponse> {
        private final Collector<Row> collector;
        private final List<String> selectFields;

        public CollectListener(Collector<Row> collector, List<String> selectFields) {
            this.collector = collector;
            this.selectFields = selectFields;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            processResponse(searchResponse, collector, selectFields);
            collector.close();
        }

        @Override
        public void onFailure(Exception e) {

        }
    }
}
