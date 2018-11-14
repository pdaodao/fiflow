package com.github.myetl.flow.core.outputformat;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * System.out.print
 */
public class SystemOutOutputFormat extends FlowOutputFormat {

    private Counter numberCounter = null;
    private int size = 0;

    public SystemOutOutputFormat(RowTypeInfo rowTypeInfo) {
        super(rowTypeInfo);
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.numberCounter = getRuntimeContext().getMetricGroup().counter(MetricNames.IO_NUM_RECORDS_IN);
        if (taskNumber != 0) return;

        TypeInformation<?> type = getProducedType();
        if (type instanceof RowTypeInfo) {
            RowTypeInfo rowTypeInfo = (RowTypeInfo) type;
            String[] names = rowTypeInfo.getFieldNames();

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rowTypeInfo.getArity(); i++) {
                sb.append(names[i] + "(" + rowTypeInfo.getTypeAt(i).toString() + ")");
                if (i < rowTypeInfo.getArity() - 1) sb.append(" | ");
            }
            System.out.println(StringUtils.repeat('-', sb.length()));
            System.out.println(sb.toString());
            System.out.println(StringUtils.repeat('-', sb.length()));
        }

    }

    @Override
    public void writeRecord(Row record) throws IOException {
        System.out.println(record.toString());
        size++;
    }

    @Override
    public void close() throws IOException {
//        System.out.println("------ size ------ "+size);
    }
}
