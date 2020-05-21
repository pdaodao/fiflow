package com.github.lessonone.fiflow.core.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

@Public
public abstract class IOSourceFunction extends RichParallelSourceFunction<Row> implements ResultTypeQueryable<Row> {

    public boolean isBounded() {
        return false;
    }
}
