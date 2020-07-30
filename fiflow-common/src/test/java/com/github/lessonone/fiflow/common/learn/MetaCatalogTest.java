package com.github.lessonone.fiflow.common.learn;

import com.github.lessonone.fiflow.common.FlinkMetaCatalog;
import com.github.lessonone.fiflow.common.base.DbInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class MetaCatalogTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        FlinkMetaCatalog metaCatalog = new FlinkMetaCatalog("meta");
        tEnv.registerCatalog("meta", metaCatalog);
        tEnv.useCatalog("meta");
        metaCatalog.addDbInfo("115",
                new DbInfo().setUrl("jdbc:mysql://10.10.77.115:3306/nani")
                        .setUsername("root").setPassword("root"));
        tEnv.useDatabase("115");


//        TableResult tableResult =  tEnv.executeSql("SELECT name, age, class from student");
//        tableResult.print();


        String insert = "insert into stuout(name, age, class) \n" +
                "   select name, age, class from student where age > 16";

        tEnv.executeSql(insert);
    }
}
