package com.github.myetl.flow.core.exception;

public class SqlFlowMetaReaderException extends SqlFlowException {

    public SqlFlowMetaReaderException() {
    }

    public SqlFlowMetaReaderException(String message) {
        super(message);
    }

    public SqlFlowMetaReaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlFlowMetaReaderException(Throwable cause) {
        super(cause);
    }

    public SqlFlowMetaReaderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
