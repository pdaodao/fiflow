package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.frame.FlowContext;

/**
 *
 */
public class SqlBuildContext {
    // 该次sql构建后是否需要执行 help 之类的完全不需要在flink中执行
    public final Boolean needExecute;
    private FlowContext flowContext;

    public SqlBuildContext(Boolean needExecute) {
        this.needExecute = needExecute;
    }

    public FlowContext getFlowContext() {
        return flowContext;
    }

    public SqlBuildContext setFlowContext(FlowContext flowContext) {
        this.flowContext = flowContext;
        return this;
    }
}
