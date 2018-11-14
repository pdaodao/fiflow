package com.github.myetl.flow.core.runtime;

import com.github.myetl.flow.core.inputformat.FlowInputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * wrap of FlowInputFormat
 */
public class InputFormatTableSource implements BatchTableSource<Row>, StreamTableSource<Row> {

    final FlowInputFormat inputFormat;

    public InputFormatTableSource(FlowInputFormat inputFormat) {
        this.inputFormat = inputFormat;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return inputFormat.getProducedType();
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(inputFormat.getProducedType().getFieldNames(), inputFormat.getProducedType().getFieldTypes());
    }

    @Override
    public String explainSource() {
        return String.format("%s (read fields: %s)", inputFormat.getClass().getCanonicalName(), StringUtils.join(inputFormat.getProducedType().getFieldNames(), ","));
    }

    @Override
    public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
        return execEnv.createInput(inputFormat);
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.createInput(inputFormat);
    }
}
