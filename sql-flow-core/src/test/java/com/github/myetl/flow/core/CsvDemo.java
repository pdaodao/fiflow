package com.github.myetl.flow.core;


public class CsvDemo {

    public static void main(String[] args) throws Exception {
        String sql = "create table person1( \n" +
                "        age int, \n" +
                "        name varchar \n" +
                ") with ( \n" +
                "        type='csv', \n" +
                "        path='sql-flow-core/src/test/resources/person.csv' \n" +
                "); \n" +
                "\n" +
                "  \n" +
                "create table person2( \n" +
                "        age int, \n" +
                "        name varchar\n" +
                ") with ( \n" +
                "        type='csv',\n" +
                "        path='sql-flow-core/src/test/resources/person2' \n" +
                ");\n" +
                "\n" +
                "\n" +
                "INSERT INTO person2 \n" +
                "SELECT \n" +
                "      age, \n" +
                "      name \n" +
                "FROM person1 where age > 2;";

        Flow flow = new Flow(sql);
        flow.execute();
    }

}
