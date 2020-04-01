package com.github.myetl.fiflow.runtime;

import com.github.myetl.fiflow.core.flink.FlinkClusterInfo;
import com.github.myetl.fiflow.core.frame.FiFlinkSession;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 把任务提交到 flink
 */
public class FiflowFlinkExecutor {

    /**
     * 本地执行
     * @param session
     * @throws Exception
     */
    public static void executeLocal(FiFlinkSession session) throws Exception{
        JobExecutionResult result = session.env.execute(session.getName());



    }

    public static void executeStandalone(FiFlinkSession session, FlinkClusterInfo flinkCluster) throws Exception{
        List<URL> jars = JarUtils.jars("mysql-connector","flink-jdbc");

        StreamGraph streamGraph = session.env.getStreamGraph(session.getName());


        RestClusterClient client = FlinkClientManager.standaloneClient();

        JobGraph jobGraph = streamGraph.getJobGraph();

        jobGraph.addJars(jars);


        JobID jobID = jobGraph.getJobID();

        System.out.println("------ job id +++++++"+jobID);
        long t1 = System.currentTimeMillis();


//        ClientUtils.submitJob(client, jobGraph);

        CompletableFuture<JobSubmissionResult> resultCompletableFuture =  client.submitJob(jobGraph);

        resultCompletableFuture.get();

        long t2 = System.currentTimeMillis();

        System.out.println("-----time(ms) :"+(t2 - t1));
    }

}
