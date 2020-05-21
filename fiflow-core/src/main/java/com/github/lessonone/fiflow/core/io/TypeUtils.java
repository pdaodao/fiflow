package com.github.lessonone.fiflow.core.io;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.TypeConversions;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TypeUtils {


    public static RowTypeInfo toNormalizeRowType(TableSchema schema) {
        final List<String> names = new ArrayList<>();
        final List<DataType> types = new ArrayList<>();

        schema.getTableColumns()
                .forEach(c -> {
                    if (!c.isGenerated()) {
                        LogicalTypeRoot root = c.getType().getLogicalType().getTypeRoot();
                        final DataType type;
                        if (root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                            type = c.getType().bridgedTo(Timestamp.class);
                        } else if (root == LogicalTypeRoot.DATE) {
                            type = c.getType().bridgedTo(Date.class);
                        } else if (root == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE) {
                            type = c.getType().bridgedTo(Time.class);
                        } else {
                            type = c.getType();
                        }
                        names.add(c.getName());
                        types.add(type);
                    }
                });


        return new RowTypeInfo(TypeConversions.fromDataTypeToLegacyInfo(types.toArray(new DataType[0])),
                names.toArray(new String[0]));

    }


}
