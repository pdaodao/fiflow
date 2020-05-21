package com.github.lessonone.fiflow.io.elasticsearch7.frame;

import com.github.lessonone.fiflow.core.io.RowCollector;
import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 连接 elasticsearch 的客户端 和 相关操作
 */
public class ESClient {
    private static final Logger LOG = LoggerFactory.getLogger(ESClient.class);

    private final ESOptions esOptions;
    private RestHighLevelClient client;
    private String scrollId = null;
    private volatile boolean running = true;

    public ESClient(ESOptions esOptions) {
        this.esOptions = esOptions;
        this.client = ESUtils.createClient(esOptions.getHosts(), esOptions.getUsername(), esOptions.getPassword());
    }

    /**
     * bulk 处理器
     *
     * @param listener
     * @return
     */
    public BulkProcessor initBulk(BulkProcessor.Listener listener) {
        return BulkProcessor.builder(
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener).build();
    }

    /**
     * 普通方式查询数据 用在 Lookup 时 单次最大获取 1w 条数据
     *
     * @param queryTemplate sql
     * @param isAsync       是否异步
     * @param collector     结果收集器
     * @throws Exception
     */
    public void search(final String queryTemplate, final boolean isAsync, final RowCollector collector) throws Exception {
        LOG.info("es search : {}", queryTemplate);
        Tuple2<SearchRequest, List<String>> build = EsSqlBuilder.build(queryTemplate);
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

    /**
     * scroll 方式获取大量数据
     *
     * @param queryTemplate
     * @param shardIndex
     * @param collector
     * @throws Exception
     */
    public void scrollSearch(String queryTemplate, Integer shardIndex, RowCollector collector) throws Exception {
        if (running == false) return;
        scrollId = null;
        LOG.info("es scroll search shard_{} : {}", shardIndex, queryTemplate);
        Tuple2<SearchRequest, List<String>> build = EsSqlBuilder.build(queryTemplate);
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
            } else {
                searchResponse = client.scroll(new SearchScrollRequest(scrollId), RequestOptions.DEFAULT);
            }
            scrollId = searchResponse.getScrollId();
            bulkSize = processResponse(searchResponse, collector, selectFields);
        } while (running && bulkSize > 0);
        clearScroll();
    }

    /**
     * 把 搜索结果转为 Row
     *
     * @param response     搜索结果
     * @param collector    收集器
     * @param selectFields 字段列表
     * @return
     */
    private int processResponse(SearchResponse response, RowCollector collector, List<String> selectFields) {
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

    /**
     * 获取分片数
     *
     * @return
     * @throws IOException
     */
    public Integer shards() throws IOException {
        return ESUtils.shards(client, esOptions.getIndex());
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
        private final RowCollector collector;
        private final List<String> selectFields;

        public CollectListener(RowCollector collector, List<String> selectFields) {
            this.collector = collector;
            this.selectFields = selectFields;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            processResponse(searchResponse, collector, selectFields);
            collector.complete();
        }

        @Override
        public void onFailure(Exception e) {
            collector.error(e);
        }
    }
}
