package com.github.myetl.flow.core.parser;

/**
 * 解析sql 时异常
 */
public class SqlParseException extends Exception {

    public SqlParseException() {
    }

    public SqlParseException(String message) {
        super(message);
    }

    public SqlParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
