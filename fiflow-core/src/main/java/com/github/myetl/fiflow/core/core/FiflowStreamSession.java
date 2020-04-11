package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.frame.SessionConfig;
import com.github.myetl.fiflow.core.stream.Flow;

public class FiflowStreamSession extends FiflowSession {

    public FiflowStreamSession(String id, SessionConfig sessionConfig) {
        super(id, sessionConfig);
    }

    public FlinkBuildInfo build(Flow flow) {



        return null;
    }
}
