package com.github.myetl.fiflow.io.elasticsearch7;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EsUtils {


    /**
     * 索引分片数
     *
     * @param client
     * @param indexName
     * @return
     * @throws IOException
     */
    public static Integer shards(RestHighLevelClient client, String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        getIndexRequest.features(GetIndexRequest.Feature.SETTINGS);

        GetIndexResponse resp = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        Settings settings = resp.getSettings().values().iterator().next();

        return settings.getAsInt("index.number_of_shards", 1);
    }


    /**
     * 创建连接 elasticsearch 的 rest 客户端
     *
     * @return
     */
    public static RestHighLevelClient createClient(String hostsandport, String username, String password) {
        String[] hosts = hostsandport.split(",");
        final List<HttpHost> httpHostList = new ArrayList<>();
        for (String h : hosts) {
            int port = 9200;
            String[] t = h.split(":");
            String host = t[0];
            if (t.length == 2) {
                port = Integer.parseInt(t[1]);
            }
            httpHostList.add(new HttpHost(host, port, "http"));
        }

        final RestClientBuilder builder = RestClient.builder(httpHostList.toArray(new HttpHost[0]));

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            final CredentialsProvider credentialsProvider =
                    new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(
                        HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }

        return new RestHighLevelClient(builder);
    }
}
