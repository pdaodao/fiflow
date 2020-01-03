package com.github.myetl.flow.core.exception;

public class SqlFlowUnsupportException extends SqlFlowException {
    public SqlFlowUnsupportException() {
    }

    public SqlFlowUnsupportException(String message) {
        super(message);
    }

    public SqlFlowUnsupportException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlFlowUnsupportException(Throwable cause) {
        super(cause);
    }

    public SqlFlowUnsupportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
