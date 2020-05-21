package com.github.lessonone.fiflow.io.elasticsearch7.demo;

/**
 * 从 mysql 中读取 数据表 student 存入 elasticsearch 的 stuout 索引中
 */
public class DemoDDL extends DemoBase {

    public static void main(String[] args) throws Exception {
        String sqlStudent = "CREATE TABLE student ( \n" +
                "    name VARCHAR, \n" +
                "    age INT, \n" +
                "   class VARCHAR, \n" +
                "   haha AS PROCTIME()\n" +
                " ) WITH ( \n" +
                "    'connector.type' = 'jdbc', \n" +
                "    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink', \n" +
                "    'connector.table' = 'student', \n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'root'\n" +
                ")";
        tEnv.sqlUpdate(sqlStudent);

        String sqlStuout = "CREATE TABLE stuout ( \n" +
                "    name VARCHAR, \n" +
                "    age INT, \n" +
                "   class VARCHAR\n" +
                " ) WITH ( \n" +
                "    'connector.type' = 'elasticsearch', \n" +
                "    'connector.hosts' = '127.0.0.1:9200', \n" +
                "    'connector.index' = 'stuout' \n" +
                ")";

        tEnv.sqlUpdate(sqlStuout);

        tEnv.sqlUpdate("insert into stuout(name,age,class) select name, age, class from student ");

        tEnv.execute("haha-job");
    }
}
