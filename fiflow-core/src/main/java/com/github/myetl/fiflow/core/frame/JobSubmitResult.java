package com.github.myetl.fiflow.core.frame;

public class JobSubmitResult {
    private final String jobId;

    public JobSubmitResult(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }
}
