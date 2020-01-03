package com.github.myetl.flow.core;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

public class JdbcSql {

    public static void main(String[] args) throws Exception {
        // 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 使用Blink
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        // table 环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        // 定义表结构信息和连接信息
        String input = "CREATE TABLE student (\n" +
                "    id INT,\n" +
                "    name VARCHAR,\n" +
                "    age INT,\n" +
                "    class VARCHAR\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink',\n" +
                "    'connector.table' = 'student',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = 'root'\n" +
                ")";

        // sqlUpdate 会把ddl按需求注册为输入输出
        tableEnv.sqlUpdate(input);
//        tableEnv.sqlUpdate(out);

//        tableEnv.registerTableSource();


//        tableEnv.connect()

        // csv 输出
        TableSink csvTableSink = new CsvTableSink("out", ",", 1, FileSystem.WriteMode.OVERWRITE)
                .configure(new String[]{"name", "age2"},
                        new TypeInformation[]{Types.STRING, Types.INT});

        // 注册为数据表
        tableEnv.registerTableSink("stuout", csvTableSink);



        tableEnv.sqlUpdate("insert into stuout select name, age, class  from student where age > 16");

        env.execute("jdbc-sql");
    }


}
