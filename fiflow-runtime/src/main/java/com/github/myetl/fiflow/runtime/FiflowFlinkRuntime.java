package com.github.myetl.fiflow.runtime;

import com.github.myetl.fiflow.core.core.FiflowRuntime;
import com.github.myetl.fiflow.core.core.SessionContext;
import com.github.myetl.fiflow.core.flink.ClusterMode;
import com.github.myetl.fiflow.core.flink.FlinkClusterInfo;
import com.github.myetl.fiflow.core.frame.JobSubmitResult;
import com.github.myetl.fiflow.core.util.JarUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;


/**
 * 在 flink 中运行
 */
public class FiflowFlinkRuntime implements FiflowRuntime {

    @Override
    public JobSubmitResult submit(SessionContext sessionContext) throws Exception {
        // 由于集群管理还没做完 这里先在代码里写一个standalone的连接信息
//        FlinkClusterInfo flinkClusterInfo = ClientInfoTest.info();

        FlinkClusterInfo flinkClusterInfo = new FlinkClusterInfo();
        flinkClusterInfo.setMode(ClusterMode.local);
        flinkClusterInfo.setCode("local1");

        sessionContext.env.setParallelism(1);

        ClusterClient client = ClientManager.getClient(flinkClusterInfo);

        StreamGraph streamGraph = sessionContext.getGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();

        if (CollectionUtils.isNotEmpty(sessionContext.getJars())) {
            jobGraph.addJars(JarUtils.jars(sessionContext.getJars().toArray(new String[0])));
        }

        JobExecutionResult result = ClientUtils.submitJob(client, jobGraph);
        String jobId = result.getJobID().toString();
        return new JobSubmitResult(jobId);
    }
}
