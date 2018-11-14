package com.github.myetl.flow.core.runtime;

import com.github.myetl.flow.core.outputformat.FlowOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * wrap of FlowOutputFormat
 */
public class OutputFormatTableSink implements BatchTableSink<Row>, AppendStreamTableSink<Row> {

    final FlowOutputFormat outputFormat;

    public OutputFormatTableSink(FlowOutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return outputFormat.getProducedType();
    }

    @Override
    public String[] getFieldNames() {
        return outputFormat.getProducedType().getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return outputFormat.getProducedType().getFieldTypes();
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new OutputFormatTableSink(outputFormat);
    }

    @Override
    public void emitDataSet(DataSet<Row> dataSet) {
        dataSet.output(outputFormat);
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.writeUsingOutputFormat(outputFormat);
    }
}
