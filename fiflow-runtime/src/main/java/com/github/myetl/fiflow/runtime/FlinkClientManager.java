package com.github.myetl.fiflow.runtime;

import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FlinkClientManager {
    private static final Configuration restConfig;

    static {
        final Configuration config = new Configuration();
//        config.setString(JobManagerOptions.ADDRESS, "10.162.12.126");
//        config.setInteger(JobManagerOptions.PORT, 6123);

        config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
        config.setLong(RestOptions.RETRY_DELAY, 3);
        config.setString(RestOptions.ADDRESS, "10.162.12.126");
        config.setInteger(RestOptions.PORT, 8081);

        restConfig = config;
    }


    public static RestClusterClient standaloneClient() throws Exception{
        ExecutorService executor  = Executors.newSingleThreadExecutor(new ExecutorThreadFactory(FlinkClientManager.class.getSimpleName()));

        final Configuration clientConfig = new Configuration(restConfig);

        RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(restConfig), executor);

        RestClusterClient<StandaloneClusterId> restClusterClient = new RestClusterClient<>(
                clientConfig,
                StandaloneClusterId.getInstance() );

//        restClusterClient.setDetached(false);

        return restClusterClient;

    }
}
