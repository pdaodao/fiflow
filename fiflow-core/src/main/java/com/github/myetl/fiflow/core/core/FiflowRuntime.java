package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.frame.FiFlinkSession;
import com.github.myetl.fiflow.core.frame.JobSubmitResult;

/**
 * 用于把 session 中 构建的任务 提交到 flink 中执行
 */
public interface FiflowRuntime {

    /**
     * 提交任务  这里只返回任务id 任务状态什么的单独获取
     * @param flowSession
     * @return
     * @throws Exception
     */
    JobSubmitResult submit(FiFlinkSession flowSession) throws Exception;
}
