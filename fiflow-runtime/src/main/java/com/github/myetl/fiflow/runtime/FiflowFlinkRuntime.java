package com.github.myetl.fiflow.runtime;

import com.github.myetl.fiflow.core.core.FiflowRuntime;
import com.github.myetl.fiflow.core.frame.FiFlinkSession;
import com.github.myetl.fiflow.core.frame.JobSubmitResult;
import com.github.myetl.fiflow.core.util.Preconditions;

/**
 * 在 flink 中运行
 */
public class FiflowFlinkRuntime implements FiflowRuntime {




    @Override
    public JobSubmitResult submit(FiFlinkSession flowSession) throws Exception {
        Preconditions.checkNotNull(flowSession, "flowsession is null");
        Preconditions.checkNotNull(flowSession.flinkClusterInfo, "cluster info is null in session "+flowSession.getId());




        return null;
    }
}
