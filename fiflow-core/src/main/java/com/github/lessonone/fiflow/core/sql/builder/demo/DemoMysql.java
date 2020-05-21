package com.github.lessonone.fiflow.core.sql.builder.demo;

public class DemoMysql extends DemoBase {

    public DemoMysql() {
        super("mysql");
    }

    @Override
    public String help() {
        return "<span style='color:green'>demo mysql</span>; jdbc simple demo";
    }

    @Override
    protected String demoFileName() {
        return "demo-mysql.txt";
    }
}
