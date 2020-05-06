package com.github.myetl.fiflow.core.sql.builder.demo;

public class DemoElasticsearch extends DemoBase {

    public DemoElasticsearch() {
        super("elasticsearch");
    }

    @Override
    public String help() {
        return "<span style='color:green'>demo elasticsearch [doing]</span>; read  and write elasticsearch";
    }

    @Override
    protected String demoFileName() {
        return "demo-elasticsearch.txt";
    }
}
