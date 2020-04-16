package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.frame.FiflowRuntimeLoader;
import com.github.myetl.fiflow.core.frame.JobSubmitResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.ArrayList;
import java.util.List;

/**
 * 若干行 sql / 一次任务 生成一个 SessionContext
 * sql 要点：
 * 对于 sql 来说 先把本次需要执行的多行 sql 预分析完毕后 利用全局信息构建 env
 * 构建sql 到 flink 的转换时 利用上一步得到的全局信息做优化 例如 谓词下推等
 */
public abstract class SessionContext {
    public final String id;
    public final StreamExecutionEnvironment env;
    private List<String> jars = new ArrayList<>();

    public SessionContext(String id, StreamExecutionEnvironment env) {
        if (StringUtils.isEmpty(id)) {
            id = RandomStringUtils.randomAlphanumeric(5);
        }
        if (env == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
        }
        this.id = id;
        this.env = env;
    }

    protected String getName() {
        return id;
    }

    public StreamGraph getGraph() {
        if (this.env == null) return null;
        return env.getStreamGraph(getName());
    }

    public void addJar(String jar) {
        this.jars.add(jar);
    }

    public List<String> getJars() {
        return jars;
    }

    public JobSubmitResult submit() throws Exception {
        FiflowRuntime fiflowRuntime = FiflowRuntimeLoader.getRuntime();
        return fiflowRuntime.submit(this);
    }
}
