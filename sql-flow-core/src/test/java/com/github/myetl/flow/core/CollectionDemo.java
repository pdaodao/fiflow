package com.github.myetl.flow.core;


public class CollectionDemo {

    public static void main(String[] args) throws Exception {

        String sql = "create table person(\n" +
                " id varchar,\n" +
                " name varchar,\n" +
                " city varchar,\n" +
                " age int\n" +
                ") with (\n" +
                " type='collection',\n" +
                " data='1,小明,1,18;2,小花,2,19;3,张三,3,20;4,李四,4,21'\n" +
                ");\n" +
                "\n" +
                "\n" +
                "create table city(\n" +
                " id varchar,\n" +
                " name varchar\n" +
                ") with (\n" +
                " type='collection',\n" +
                " data='1,北京;2,上海;3,杭州;4,香港'\n" +
                ");\n" +
                "\n" +
                "\n" +
                "create table fullinfo(\n" +
                " name varchar,\n" +
                " age int,\n" +
                " city varchar\n" +
                ") with (\n" +
                "  type='collection'\n" +
                ");\n" +
                "\n" +
                "\n" +
                "\n" +
                "INSERT INTO fullinfo\n" +
                "SELECT\n" +
                "   name,\n" +
                "   age,\n" +
                "   cityname\n" +
                "FROM\n" +
                "   (SELECT a.name as name , \n" +
                "           a.age as age, \n" +
                "           b.name as cityname \n" +
                "    FROM person AS a \n" +
                "    LEFT JOIN city AS b on a.city = b.id\n" +
                "   ); ";
        Flow flow = new Flow(sql);
        flow.execute();

    }
}
