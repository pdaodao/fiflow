package com.github.myetl.flow.core.exception;


public class SqlRunException extends FlowException {
    public SqlRunException() {
    }

    public SqlRunException(String message) {
        super(message);
    }

    public SqlRunException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlRunException(Throwable cause) {
        super(cause);
    }
}
