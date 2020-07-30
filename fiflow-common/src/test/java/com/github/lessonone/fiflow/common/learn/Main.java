package com.github.lessonone.fiflow.common.learn;

import com.github.lessonone.fiflow.common.base.DbInfo;
import com.github.lessonone.fiflow.common.FlinkInDbCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {

    /**
     *  GenericInMemoryCatalog inMemoryCatalog = new GenericInMemoryCatalog("haha");
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        final String InDbCatalog = "in_db";
        DbInfo dbInfo = new DbInfo()
                .setUrl("jdbc:mysql://10.12.102.110:3306/flink")
                .setUsername("root")
                .setPassword("root")
                .setDriverClassName("com.mysql.cj.jdbc.Driver");

        tEnv.registerCatalog(InDbCatalog, new FlinkInDbCatalog(InDbCatalog, dbInfo));

        tEnv.useCatalog(InDbCatalog);

        String[] cs = tEnv.listCatalogs();

        String[] ts = tEnv.listDatabases();

        String ddl1 = " CREATE TABLE student ( \n" +
                "    name VARCHAR, \n" +
                "    age INT, \n" +
                "   class VARCHAR \n" +
                " ) WITH ( \n" +
                "    'connector.type' = 'jdbc', \n" +
                "    'connector.url' = 'jdbc:mysql://10.10.77.115:3306/nani', \n" +
                "    'connector.table' = 'student', \n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'root'\n" +
                ")";
        String ddl2 = "CREATE TABLE stuout ( \n" +
                "    name VARCHAR, \n" +
                "    age INT, \n" +
                "   class VARCHAR \n" +
                " ) WITH ( \n" +
                "    'connector.type' = 'jdbc', \n" +
                "    'connector.url' = 'jdbc:mysql://10.10.77.115:3306/nani', \n" +
                "    'connector.table' = 'stuout', \n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'root'\n" +
                ")";

        String insert = "insert into stuout(name,age,class) \n" +
                "   select name, age, class from student where age > 16";

        tEnv.executeSql(ddl1);
        tEnv.executeSql(ddl2);

//        tEnv.executeSql(insert);

//        tEnv.sqlUpdate(insert);
//

//        tEnv.execute("job test");
    }
}
