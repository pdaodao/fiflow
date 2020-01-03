package com.github.myetl.flow.core.meta;

import com.github.myetl.flow.core.exception.SqlFlowMetaFieldTypeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class BasicFieldTypeConverter {


    /**
     * 字段类型转换
     *
     * @param typeName    字段类型名称  如 INT UNSIGNED
     * @param fieldSize   字段长度
     * @param fieldDigits 小数精度
     * @return
     */
    public static DbFieldType convert(String typeName, Integer fieldSize, Integer fieldDigits) throws SqlFlowMetaFieldTypeException {
        if (StringUtils.isEmpty(typeName)) return DbFieldType.create(DbFieldTypeName.String);
        typeName = typeName.trim().toLowerCase();

        DbFieldTypeName name = null;

        for(DbFieldTypeName type : DbFieldTypeName.values()){
            int index = type.keys.indexOf(typeName);
            if(index > -1){
                if(index == 0 || type.keys.charAt(index-1) == ',' || type.keys.charAt(index-1) == ' '){
                    name = type;
                    break;
                }
            }
        }

        if (name == null) {
            throw new SqlFlowMetaFieldTypeException("to DbFieldType convert, unknown field type :" + typeName);
        }
        return DbFieldType.create(name, fieldSize, fieldDigits);
    }


    /**
     * 转换为 flink 内部的字段类型
     *
     * @param dbFieldType
     * @return
     * @throws SqlFlowMetaFieldTypeException
     */
    public static DataType toFlinkType(DbFieldType dbFieldType) throws SqlFlowMetaFieldTypeException {
        if (dbFieldType == null || dbFieldType.typeName == null)
            return DataTypes.STRING();

        switch (dbFieldType.typeName) {
            case String:
                return DataTypes.STRING();
            case Varchar:
                return DataTypes.VARCHAR(dbFieldType.size);
            case Tinyint: return DataTypes.TINYINT();
            case Int: return DataTypes.INT();
            case Bigint:
                return DataTypes.BIGINT();
            case Decimal:
                return DataTypes.DECIMAL(dbFieldType.size, dbFieldType.digits);
            case Float:
                return DataTypes.FLOAT();
            case Boolean:
                return DataTypes.BOOLEAN();
            case Datetime:
                return DataTypes.DATE();
            case Timestamp:
                return DataTypes.TIMESTAMP();
        }
        throw new SqlFlowMetaFieldTypeException("to flink type convert exception, unknown type:" + dbFieldType);
    }


}
