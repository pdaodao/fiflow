package com.github.myetl.flow.core;


import com.github.myetl.flow.core.inputformat.CollectionInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public class SimpleJoinTest {

    public static class Person{
        private final String id;
        private final String name;
        private final String cityId;

        public Person(String id, String name, String cityId) {
            this.id = id;
            this.name = name;
            this.cityId = cityId;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getCityId() {
            return cityId;
        }
    }

    public static class City{
        private final String id;
        private final String name;

        public City(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    public static void main(String[] args) throws Exception{

        Person[] personList = new Person[]{
                new Person("1", "张三", "1"),
                new Person("2", "李四", "2")
        };

        City[] cityList = new City[]{
                new City("1", "北京"),
                new City("2", "上海")
        };


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Row> person = env.createInput(CollectionInputFormat.builder(env.getConfig(), personList).build());
        DataSet<Row>  city = env.createInput(CollectionInputFormat.builder(env.getConfig(), cityList).build());


        DataSet result = person.join(city).where(2).equalTo(0);

        result.print();

    }
}
