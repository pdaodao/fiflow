package com.github.lessonone.fiflow.core.stream;

import com.github.lessonone.fiflow.core.core.FiflowSession;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.frame.SessionConfig;

public class FiflowStreamSession extends FiflowSession {

    public FiflowStreamSession(String id, SessionConfig sessionConfig) {
        super(id, sessionConfig);
    }


    public FlinkBuildInfo build(Flow flow) {


        return null;
    }
}
