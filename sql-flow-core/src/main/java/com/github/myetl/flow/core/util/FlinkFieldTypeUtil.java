package com.github.myetl.flow.core.util;


import com.github.myetl.flow.core.enums.FieldType;
import com.github.myetl.flow.core.parser.DDL;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.util.CollectionUtil;
import scala.tools.nsc.backend.jvm.BTypes;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * field type util
 */
public class FlinkFieldTypeUtil {

    public static RowTypeInfo toFlinkType(DDL ddl) {
        if (ddl == null || CollectionUtil.isNullOrEmpty(ddl.getFields()))
            return null;
        String[] fields = new String[ddl.getFields().size()];
        TypeInformation[] types = new TypeInformation[ddl.getFields().size()];

        int i = 0;
        for (DDL.DDLFieldInfo fieldInfo : ddl.getFields()) {
            fields[i] = fieldInfo.getName();
            types[i++] = toFlinkType(fieldInfo.getType());
        }
        return new RowTypeInfo(types, fields);
    }


    private static TypeInformation toFlinkType(FieldType fieldType) {
        switch (fieldType) {
            case VARCHAR:
                return STRING_TYPE_INFO;
            case BOOLEAN:
                return BOOLEAN_TYPE_INFO;
            case TINYINT:
            case SMALLINT:
                return SHORT_TYPE_INFO;
            case INT:
                return INT_TYPE_INFO;
            case LONG:
                return LONG_TYPE_INFO;
            case BIGINT:
                return BIG_INT_TYPE_INFO;
            case FLOAT:
                return FLOAT_TYPE_INFO;
            case DECIMAL:
            case DOUBLE:
                return DOUBLE_TYPE_INFO;
            case DATE:
                return SqlTimeTypeInfo.DATE;
            case TIME:
                return SqlTimeTypeInfo.TIME;
            case TIMESTAMP:
                return SqlTimeTypeInfo.TIMESTAMP;
            case VARBINARY:
                return BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
        }
        return STRING_TYPE_INFO;
    }

}
