package com.github.lessonone.fiflow.io.elasticsearch7.demo;

import com.github.lessonone.fiflow.io.elasticsearch7.core.ESOptions;
import com.github.lessonone.fiflow.io.elasticsearch7.sink.ESSinkFunction;
import com.github.lessonone.fiflow.io.elasticsearch7.source.ESSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * 从 elasticsearch 的 student 索引 读取数据 写入到 stuout 索引中
 */
public class DemoStream extends DemoBase {

    public static void main(String[] args) throws Exception {

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("class", DataTypes.STRING()).build();

        ESOptions es1 = ESOptions.builder()
                .setHosts("127.0.0.1:9200")
                .setIndex("student")
                .build();

        ESOptions es2 = ESOptions.builder()
                .setHosts("127.0.0.1:9200")
                .setIndex("stuout")
                .build();

        // source function
        ESSourceFunction sourceFunction = ESSourceFunction.builder()
                .setEsOptions(es1)
                .setRowTypeInfo(tableSchema)
                .build();

        // sink function
        ESSinkFunction sinkFunction = ESSinkFunction.builder()
                .setEsOptions(es2)
                .setRowTypeInfo(tableSchema)
                .build();


        // 1. add source
        DataStreamSource<Row> in = env.addSource(sourceFunction);
        in.setParallelism(1);

        // 2. filter then sink
        in.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return (int) value.getField(1) > 3;
            }
        }).addSink(sinkFunction);

        env.execute("haha");
    }
}
