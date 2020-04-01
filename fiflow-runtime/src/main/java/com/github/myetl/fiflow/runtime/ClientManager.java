package com.github.myetl.fiflow.runtime;

import com.github.myetl.fiflow.core.flink.FlinkClusterInfo;
import com.github.myetl.fiflow.core.flink.FlinkMode;
import com.github.myetl.fiflow.core.util.Preconditions;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * flink 客户端管理
 */
public class ClientManager {
    private static Map<String, ClusterClient> clientMap = new ConcurrentHashMap<>();
    private static ExecutorService executor;

    public static synchronized ClusterClient getClient(FlinkClusterInfo clusterInfo) throws Exception{
        Preconditions.checkNotNull(clusterInfo, "flink cluster info is null");

        if(executor == null){
            executor = Executors.newCachedThreadPool(new ExecutorThreadFactory(FlinkClientManager.class.getSimpleName()));
        }

        if(!clientMap.containsKey(clusterInfo.getCode())){
            if(clusterInfo.getMode()  == FlinkMode.standalone){
                final Configuration config = new Configuration();
                config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
                config.setLong(RestOptions.RETRY_DELAY, 3);
                config.setString(RestOptions.ADDRESS, clusterInfo.getHost());
                config.setInteger(RestOptions.PORT, clusterInfo.getPort());

                RestClusterClient restClient = new RestClusterClient(config, executor);
                clientMap.put(clusterInfo.getCode(), restClient);
            }else {
                // local
            }
        }

        return clientMap.get(clusterInfo.getCode());
    }

}
