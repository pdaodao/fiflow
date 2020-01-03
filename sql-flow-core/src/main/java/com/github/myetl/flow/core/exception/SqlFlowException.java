package com.github.myetl.flow.core.exception;

public class SqlFlowException extends Exception {

    public SqlFlowException() {
    }

    public SqlFlowException(String message) {
        super(message);
    }

    public SqlFlowException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlFlowException(Throwable cause) {
        super(cause);
    }

    public SqlFlowException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
