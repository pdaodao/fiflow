package com.github.myetl.flow.core.exception;

public class SqlFlowConnectorBuildException extends SqlFlowException {

    public SqlFlowConnectorBuildException() {
    }

    public SqlFlowConnectorBuildException(String message) {
        super(message);
    }

    public SqlFlowConnectorBuildException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlFlowConnectorBuildException(Throwable cause) {
        super(cause);
    }

    public SqlFlowConnectorBuildException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
