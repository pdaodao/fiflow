package com.github.myetl.fiflow.core.sql.builder.demo;

public class DemoMysql extends DemoBase {

    public DemoMysql() {
        super("mysql");
    }

    @Override
    public String help() {
        return "demo mysql; jdbc simple demo";
    }

    @Override
    protected String demoFileName() {
        return "demo-mysql.txt";
    }
}
