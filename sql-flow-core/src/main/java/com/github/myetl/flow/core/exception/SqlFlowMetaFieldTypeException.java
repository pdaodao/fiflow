package com.github.myetl.flow.core.exception;

public class SqlFlowMetaFieldTypeException extends SqlFlowMetaReaderException {
    public SqlFlowMetaFieldTypeException() {
    }

    public SqlFlowMetaFieldTypeException(String message) {
        super(message);
    }

    public SqlFlowMetaFieldTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlFlowMetaFieldTypeException(Throwable cause) {
        super(cause);
    }

    public SqlFlowMetaFieldTypeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
