package com.github.myetl.fiflow.core.sql.builder.demo;


public class DemoKafka extends DemoBase {

    public DemoKafka() {
        super("kafka");
    }

    @Override
    public String help() {
        return "demo kafka; read from kafka then sink to mysql simple demo";
    }

    @Override
    protected String demoFileName() {
        return "demo-kafka.txt";
    }
}
