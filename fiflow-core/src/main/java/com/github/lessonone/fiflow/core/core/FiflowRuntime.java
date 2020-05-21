package com.github.lessonone.fiflow.core.core;

import com.github.lessonone.fiflow.core.frame.JobSubmitResult;

/**
 * 用于把 session 中 构建的任务 提交到 flink 中执行
 */
public interface FiflowRuntime {

    /**
     * 提交任务  这里只返回任务id 任务状态什么的单独获取
     *
     * @param sessionContext
     * @return
     * @throws Exception
     */
    JobSubmitResult submit(SessionContext sessionContext) throws Exception;
}
