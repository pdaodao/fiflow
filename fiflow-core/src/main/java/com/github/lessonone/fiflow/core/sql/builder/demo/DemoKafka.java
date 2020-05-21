package com.github.lessonone.fiflow.core.sql.builder.demo;


public class DemoKafka extends DemoBase {

    public DemoKafka() {
        super("kafka");
    }

    @Override
    public String help() {
        return "<span style='color:green'>demo kafka</span>; read from kafka then sink to mysql simple demo";
    }

    @Override
    protected String demoFileName() {
        return "demo-kafka.txt";
    }
}
