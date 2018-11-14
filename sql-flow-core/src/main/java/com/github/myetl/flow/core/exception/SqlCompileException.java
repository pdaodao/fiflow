package com.github.myetl.flow.core.exception;

/**
 * sql to flink compile exception
 */
public class SqlCompileException extends FlowException {

    public SqlCompileException() {
    }

    public SqlCompileException(String message) {
        super(message);
    }

    public SqlCompileException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlCompileException(Throwable cause) {
        super(cause);
    }
}
