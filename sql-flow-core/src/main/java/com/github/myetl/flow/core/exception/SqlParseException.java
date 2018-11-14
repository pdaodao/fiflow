package com.github.myetl.flow.core.exception;

/**
 * 解析sql 时异常
 */
public class SqlParseException extends FlowException {

    public SqlParseException() {
    }

    public SqlParseException(String message) {
        super(message);
    }

    public SqlParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
