package com.github.myetl.flow.core.parser;

/**
 * udf : CREATE FUNCTION stringLengthUdf AS 'com.hjc.test.blink.sql.udx.StringLengthUdf';
 */
public class UDF {
    private final String funcName;
    private final String className;

    public UDF(String funcName, String className) {
        this.funcName = funcName;
        this.className = className;
    }

    public String getFuncName() {
        return funcName;
    }

    public String getClassName() {
        return className;
    }
}
