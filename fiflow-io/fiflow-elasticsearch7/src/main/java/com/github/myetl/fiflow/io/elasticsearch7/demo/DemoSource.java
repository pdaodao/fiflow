package com.github.myetl.fiflow.io.elasticsearch7.demo;

import com.github.myetl.fiflow.core.util.FlinkUtils;
import com.github.myetl.fiflow.io.elasticsearch7.ES;
import com.github.myetl.fiflow.io.elasticsearch7.core.ESOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;

/**
 * 从 elasticsearch 中读取数据 谓词下推
 */
public class DemoSource {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment
                .create(env, EnvironmentSettings
                        .newInstance()
                        .inStreamingMode()
                        .useBlinkPlanner()
                        .build());

        ESOptions esOptions = ESOptions.builder()
                .setHosts("127.0.0.1:9200")
                .setIndex("student")
                .build();

        Schema tableSchema = new Schema()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("class", DataTypes.STRING());

        tEnv.connect(new ES().setEsOptions(esOptions))
                .withSchema(tableSchema)
                .createTemporaryTable("student");

        Table table = tEnv.sqlQuery("select name, age, class from student where age >= 5");

        FlinkUtils.collect(table);

        tEnv.execute("haha-job");
    }
}
