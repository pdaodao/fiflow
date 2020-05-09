package com.github.myetl.fiflow.core.io;

import org.apache.flink.types.Row;

public interface RowCollector {

    void collect(Row record);

    public default void complete() {

    }

    public default void error(Exception e) {

    }

}
