package com.github.lessonone.fiflow.core.sql.builder.demo;

public class DemoMysqlBinlog extends DemoBase {

    public DemoMysqlBinlog() {
        super("mysql-binlog");
    }

    @Override
    public String help() {
        return "<span style='color:green'>demo mysql-binlog [doing]</span>; read mysql binlog";
    }

    @Override
    protected String demoFileName() {
        return "demo-mysql-binlog.txt";
    }
}

