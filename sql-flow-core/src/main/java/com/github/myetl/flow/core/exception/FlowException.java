package com.github.myetl.flow.core.exception;


public class FlowException extends Exception {

    public FlowException() {
    }

    public FlowException(String message) {
        super(message);
    }

    public FlowException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlowException(Throwable cause) {
        super(cause);
    }
}
