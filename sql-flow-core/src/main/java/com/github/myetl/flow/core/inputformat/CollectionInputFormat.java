package com.github.myetl.flow.core.inputformat;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 测试时使用 数组数据集
 */
public class CollectionInputFormat extends FlowInputFormat implements NonParallelInput {

    private final List<Row> rows;

    private transient Iterator<Row> iterator;

    public CollectionInputFormat(RowTypeInfo rowTypeInfo, List<Row> rows) {
        super(rowTypeInfo);
        this.rows = rows;
    }

    @Override
    public void open(InputSplit split) throws IOException {

        System.out.println("CollectionInputFormat open " + split.getSplitNumber());
        this.iterator = this.rows.iterator();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !this.iterator.hasNext();
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        return this.iterator.next();
    }

    @Override
    public void close() throws IOException {
        this.rows.clear();
    }

    @Override
    public String toString() {
        return StringUtils.join(this.rows, "\n");
    }
}