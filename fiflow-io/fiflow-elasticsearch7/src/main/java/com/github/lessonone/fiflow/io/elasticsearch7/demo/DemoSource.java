package com.github.lessonone.fiflow.io.elasticsearch7.demo;

import com.github.lessonone.fiflow.core.util.FlinkUtils;
import com.github.lessonone.fiflow.io.elasticsearch7.ES;
import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptors.Schema;

/**
 * 从 elasticsearch 中读取数据 谓词下推
 */
public class DemoSource extends DemoBase {

    public static void main(String[] args) throws Exception {
        ESOptions esOptions = ESOptions.builder()
                .setHosts("127.0.0.1:9200")
                .setIndex("student")
                .build();

        Schema tableSchema = new Schema()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("class", DataTypes.STRING());

        tEnv.connect(new ES()
                .setEsOptions(esOptions))
                .withSchema(tableSchema)
                .createTemporaryTable("student");

        Table table = tEnv.sqlQuery("select name, age, class from student where age >= 5");

        FlinkUtils.collect(table);

        tEnv.execute("haha-job");
    }
}
