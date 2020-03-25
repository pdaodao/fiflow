package com.github.myetl.fiflow.runtime;

import com.github.myetl.fiflow.core.frame.FlowSession;
import org.apache.flink.api.common.JobExecutionResult;

public class FiflowRunner {

    /**
     * 本地执行
     * @param session
     * @throws Exception
     */
    public static void executeLocal(FlowSession session) throws Exception{
        JobExecutionResult result = session.env.execute("test");
    }

}
