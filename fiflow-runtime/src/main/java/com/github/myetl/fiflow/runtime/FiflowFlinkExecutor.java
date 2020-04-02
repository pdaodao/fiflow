package com.github.myetl.fiflow.runtime;

/**
 * 把任务提交到 flink
 */
public class FiflowFlinkExecutor {

    /**
     * 本地执行
     *
     * @param session
     * @throws Exception
     */
//    public static void executeLocal(FiFlinkSession session) throws Exception {
//        JobExecutionResult result = session.env.execute(session.getName());
//
//
//    }

//    public static void executeStandalone(FiFlinkSession session, FlinkClusterInfo flinkCluster) throws Exception {
//        List<URL> jars = JarUtils.jars("mysql-connector", "flink-jdbc");
//
//        StreamGraph streamGraph = session.env.getStreamGraph(session.getName());
//
//
//        RestClusterClient client = FlinkClientManager.standaloneClient();
//
//        JobGraph jobGraph = streamGraph.getJobGraph();
//
//        jobGraph.addJars(jars);
//
//
//        JobID jobID = jobGraph.getJobID();
//
//        System.out.println("------ job id +++++++" + jobID);
//        long t1 = System.currentTimeMillis();
//
//
////        ClientUtils.submitJob(client, jobGraph);
//
//        CompletableFuture<JobSubmissionResult> resultCompletableFuture = client.submitJob(jobGraph);
//
//        resultCompletableFuture.get();
//
//        long t2 = System.currentTimeMillis();
//
//        System.out.println("-----time(ms) :" + (t2 - t1));
//    }

}
