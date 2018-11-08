package com.github.myetl.flow.core.runtime;

/**
 * sql to flink compile exception
 */
public class SqlCompileException extends Exception {

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
