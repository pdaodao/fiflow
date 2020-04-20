package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.frame.FiflowRuntimeLoader;
import com.github.myetl.fiflow.core.frame.JobSubmitResult;
import com.github.myetl.fiflow.core.util.JarUtils;
import com.github.myetl.fiflow.core.util.Preconditions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 若干行 sql / 一次任务 生成一个 SessionContext
 * sql 要点：
 * 对于 sql 来说 先把本次需要执行的多行 sql 预分析完毕后 利用全局信息构建 env
 * 构建sql 到 flink 的转换时 利用上一步得到的全局信息做优化 例如 谓词下推等
 */
public abstract class SessionContext<T extends FiflowSession> {
    public final String id;
    public final T session;
    public final StreamExecutionEnvironment env;
    public final boolean isNewCreatedEnv;

    private List<String> jars = new ArrayList<>();
    private String jobId;
    private boolean isBuild = false;

    /**
     * @param id      该 context 的  id
     * @param session FiflowSession
     * @param env     StreamExecutionEnvironment
     */
    public SessionContext(String id, T session, StreamExecutionEnvironment env) {
        Preconditions.checkNotNull(session, "new SessionContext, session is null");
        this.session = session;
        if (id == null) {
            id = session.incrementAndGetContextId();
        }
        this.id = id;
        if (env == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            int p = session.sessionConfig.parallelism;
            env.setParallelism(p);
            this.isNewCreatedEnv = true;
        } else {
            this.isNewCreatedEnv = false;
        }
        this.env = env;
    }

    public SessionContext(T session, StreamExecutionEnvironment env) {
        this(null, session, env);
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


    public List<URL> getJarFile() throws IOException {
        if (this.jars == null) return new ArrayList<>();
        String[] js = this.jars.toArray(new String[0]);

        return JarUtils.jars(js);
    }

    public String getJobId() {
        return jobId;
    }

    /**
     * 转换成 flink 中的操作
     *
     * @return
     */
    public FlinkBuildInfo build() {
        if (this.isBuild == true) {
            throw new IllegalArgumentException(this.getClass().getSimpleName() + "-" + id + " already build");
        }
        this.isBuild = true;
        return null;
    }

    /**
     * 是否需要提交执行
     *
     * @return
     */
    protected abstract boolean isNeedSubmit();

    /**
     * 提交任务
     *
     * @return
     * @throws Exception
     */
    public JobSubmitResult submit() throws Exception {
        if (this.isBuild == false) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " please build before submit");
        }

        if (isNeedSubmit() == false) {
            throw new IllegalArgumentException("SessionContext no need to submit job:" + id);
        }

        FiflowRuntime fiflowRuntime = FiflowRuntimeLoader.getRuntime();
        JobSubmitResult ret = fiflowRuntime.submit(this);
        this.jobId = ret.getJobId();
        return ret;
    }
}
