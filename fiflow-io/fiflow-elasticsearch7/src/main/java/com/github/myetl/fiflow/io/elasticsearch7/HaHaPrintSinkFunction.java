package com.github.myetl.fiflow.io.elasticsearch7;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

public class HaHaPrintSinkFunction extends RichSinkFunction<Row> {
    @Override
    public void invoke(Row value, Context context) throws Exception {
        System.out.println("哈哈 ---- : " + value.toString());
    }
}
