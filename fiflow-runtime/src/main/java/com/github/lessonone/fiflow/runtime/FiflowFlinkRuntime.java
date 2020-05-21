package com.github.lessonone.fiflow.runtime;

import com.github.lessonone.fiflow.core.core.FiflowRuntime;
import com.github.lessonone.fiflow.core.core.SessionContext;
import com.github.lessonone.fiflow.core.flink.ClusterMode;
import com.github.lessonone.fiflow.core.flink.FlinkClusterInfo;
import com.github.lessonone.fiflow.core.frame.JobSubmitResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
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
        StreamGraph streamGraph = sessionContext.getGraph();

        if (flinkClusterInfo.getMode() == ClusterMode.local) {
            return local(streamGraph);
        } else if (flinkClusterInfo.getMode() == ClusterMode.standalone) {
            return standAlone(sessionContext, flinkClusterInfo, streamGraph);
        }
        throw new UnsupportedOperationException("unsupport ");
    }

    private JobSubmitResult standAlone(SessionContext sessionContext, FlinkClusterInfo flinkClusterInfo, StreamGraph streamGraph) throws Exception {
        JobGraph jobGraph = streamGraph.getJobGraph();

        // 添加依赖 jar 包
        jobGraph.addJars(sessionContext.getJarFile());

        ClusterClient client = ClientManager.getClient(flinkClusterInfo);


        JobExecutionResult result = ClientUtils.submitJob(client, jobGraph);

        String jobId = result.getJobID().toString();
        return new JobSubmitResult(jobId);

    }

    private JobSubmitResult local(StreamGraph streamGraph) throws Exception {
        LocalStreamEnvironment local = LocalStreamEnvironment.createLocalEnvironment();
        JobClient client = local.executeAsync(streamGraph);
        String jobId = client.getJobID().toString();
        return new JobSubmitResult(jobId);
    }
}
