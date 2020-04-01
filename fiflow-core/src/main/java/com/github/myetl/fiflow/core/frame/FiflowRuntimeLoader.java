package com.github.myetl.fiflow.core.frame;

import com.github.myetl.fiflow.core.core.FiflowRuntime;

import java.util.ServiceLoader;

/**
 * 加载 fiflow flink 运行时
 */
public class FiflowRuntimeLoader {

    /**
     * 获取 fiflow 运行时
     * @return
     */
    public static FiflowRuntime getRuntime(){
        ServiceLoader<FiflowRuntime> runtime = ServiceLoader.load(FiflowRuntime.class);
        if(!runtime.iterator().hasNext()){
            throw new RuntimeException("no fiflow runtime provided.");
        }
        return runtime.iterator().next();
    }
}
