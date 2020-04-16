package com.github.myetl.fiflow.core.stream;

import com.github.myetl.fiflow.core.core.FiflowSession;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.frame.SessionConfig;

public class FiflowStreamSession extends FiflowSession {

    public FiflowStreamSession(String id, SessionConfig sessionConfig) {
        super(id, sessionConfig);
    }


    public FlinkBuildInfo build(Flow flow) {


        return null;
    }
}
