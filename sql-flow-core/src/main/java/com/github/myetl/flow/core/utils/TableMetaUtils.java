package com.github.myetl.flow.core.utils;

import com.github.myetl.flow.core.exception.SqlFlowMetaFieldTypeException;
import com.github.myetl.flow.core.meta.BasicFieldTypeConverter;
import com.github.myetl.flow.core.meta.DBTableField;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

public class TableMetaUtils {

    /**
     * 把字段信息 转成 table api 所需的 TableSchema
     *
     * @param fields 字段列表 ( 字段名称 字段类型 )
     * @return
     * @throws SqlFlowMetaFieldTypeException
     */
    public static TableSchema toTableSchema(List<DBTableField> fields) throws SqlFlowMetaFieldTypeException {
        TableSchema.Builder builder = TableSchema.builder();

        for (DBTableField field : fields) {
            builder.field(field.getName(), BasicFieldTypeConverter.toFlinkType(field.getFieldType()));
        }

        return builder.build();
    }
}
