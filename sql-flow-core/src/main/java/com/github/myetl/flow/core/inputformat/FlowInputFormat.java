package com.github.myetl.flow.core.inputformat;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * 输入
 */
public abstract class FlowInputFormat extends RichInputFormat<Row, InputSplit>
        implements ResultTypeQueryable<Row> {

    protected RowTypeInfo rowTypeInfo;

    protected FlowInputFormat(RowTypeInfo rowTypeInfo) {
        if (rowTypeInfo == null) {
            throw new IllegalArgumentException("FlowInputFormat rowTypeInfo should be given by manu.");
        }
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int numSplits) throws IOException {
        if (numSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        numSplits = (this instanceof NonParallelInput) ? 1 : numSplits;
        GenericInputSplit[] splits = new GenericInputSplit[numSplits];
        for (int i = 0; i < splits.length; i++) {
            splits[i] = new GenericInputSplit(i, numSplits);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public RowTypeInfo getProducedType() {
        return rowTypeInfo;
    }

}
