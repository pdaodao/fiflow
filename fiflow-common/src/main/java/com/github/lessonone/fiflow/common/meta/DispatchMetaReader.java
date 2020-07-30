package com.github.lessonone.fiflow.common.meta;

import com.github.lessonone.fiflow.common.MetaReader;
import com.github.lessonone.fiflow.common.base.DbInfo;
import com.github.lessonone.fiflow.common.base.TableInfo;
import com.github.lessonone.fiflow.common.catalog.FlinkCatalogTable;
import com.github.lessonone.fiflow.common.exception.AutoMetaNotSupportException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import java.util.HashMap;
import java.util.Map;

public class DispatchMetaReader {

    public static TableSchema toTableSchema(TableInfo tableInfo) {
        TableSchema.Builder builder = TableSchema.builder();
        for (TableInfo.TableColumn column : tableInfo.getColumns()) {
            builder.field(column.getName(), jdbcTypeConvert(column));
        }

        return builder.build();
    }

    /**
     * jdbc column type convert to flink data type
     *
     * @param column
     * @return
     */
    public static DataType jdbcTypeConvert(TableInfo.TableColumn column) {
        String typeName = column.getType().toLowerCase();
        Integer size = column.getSize();
        Integer digits = column.getDigits();

        if (typeName.contains("boolean")) {
            if (typeName.contains("array")) return DataTypes.ARRAY(DataTypes.BOOLEAN());
            return DataTypes.BOOLEAN();
        }

        if (typeName.contains("timestamp")) {
            if (typeName.contains("timestamptz")) {
                if (typeName.contains("_")) {
                    return DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(size));
                }
                if (size > 0)
                    return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(size);
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            }

            if (typeName.contains("_")) {
                if (size > 0)
                    return DataTypes.ARRAY(DataTypes.TIMESTAMP(size));
                return DataTypes.ARRAY(DataTypes.TIMESTAMP(size));
            }

            if (size > 0)
                return DataTypes.TIMESTAMP(size);
            return DataTypes.TIMESTAMP();
        }

        if (typeName.contains("date")) {
            if (typeName.contains("_"))
                return DataTypes.ARRAY(DataTypes.DATE());
            return DataTypes.DATE();
        }

        if (typeName.contains("byte")) {
            if (typeName.contains("_bytea"))
                return DataTypes.ARRAY(DataTypes.BYTES());
            return DataTypes.BYTES();
        }

        if (typeName.contains("int") || typeName.contains("serial")) {
            if (typeName.contains("_int2"))
                return DataTypes.ARRAY(DataTypes.SMALLINT());
            if (typeName.contains("_int4"))
                return DataTypes.ARRAY(DataTypes.INT());
            if (typeName.contains("int8") || typeName.contains("bigserial"))
                return DataTypes.BIGINT();
            if (typeName.contains("_int8"))
                return DataTypes.ARRAY(DataTypes.BIGINT());
            if (typeName.contains("big"))
                return DataTypes.BIGINT();
            if (typeName.contains("small") || typeName.contains("int2"))
                return DataTypes.SMALLINT();
            if (typeName.contains("tiny"))
                return DataTypes.TINYINT();
            if (typeName.contains("unsigned"))
                return DataTypes.BIGINT();

            return DataTypes.INT();
        }

        if (typeName.contains("float")) {
            if (typeName.contains("_float4"))
                return DataTypes.ARRAY(DataTypes.FLOAT());
            if (typeName.contains("float8"))
                return DataTypes.DOUBLE();
            if (typeName.contains("_float8"))
                return DataTypes.ARRAY(DataTypes.DOUBLE());
            return DataTypes.FLOAT();
        }

        if (typeName.contains("numeric")) {
            if (typeName.contains("_numeric")) {
                if (size > 0) {
                    return DataTypes.ARRAY(DataTypes.DECIMAL(size, digits));
                }
                return DataTypes.ARRAY(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18));
            }

            if (size > 0) {
                return DataTypes.DECIMAL(size, digits);
            }
            return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18);
        }

        if (typeName.contains("char")) {
            if (typeName.contains("_varchar"))
                return DataTypes.ARRAY(DataTypes.VARCHAR(size));
            if (typeName.contains("_"))
                return DataTypes.ARRAY(DataTypes.CHAR(size));

            if (typeName.contains("varchar"))
                DataTypes.VARCHAR(size);
            return DataTypes.CHAR(size);
        }

        if (typeName.contains("text") || typeName.contains("document")) {
            if (typeName.contains("_"))
                return DataTypes.ARRAY(DataTypes.STRING());
            return DataTypes.STRING();
        }


        throw new UnsupportedOperationException(
                String.format("Doesn't support jdbc column type '%s' yet", typeName));

    }

    public MetaReader getMetaReader(DbInfo dbInfo) throws AutoMetaNotSupportException {
        MetaReader metaReader = new JdbcBaseMetaReader(dbInfo);
        if (metaReader == null) {
            throw new AutoMetaNotSupportException("connector " + dbInfo.getUrl() + " not support auto meta read");
        }
        return metaReader;
    }

    public CatalogBaseTable getTable(DbInfo dbInfo, ObjectPath tablePath) throws TableNotExistException, CatalogException {
        MetaReader metaReader = getMetaReader(dbInfo);
        TableInfo tableInfo = metaReader.getTable(tablePath.getObjectName());

        TableSchema tableSchema = toTableSchema(tableInfo);
        Map<String, String> properties = new HashMap<>();

        properties.put("connector.type", "jdbc");
        properties.put("connector.url", dbInfo.getUrl());
        properties.put("connector.table", tablePath.getObjectName());
        properties.put("connector.username", dbInfo.getUsername());
        properties.put("connector.password", dbInfo.getPassword());

        return new FlinkCatalogTable(null, tableSchema, properties, null);
    }


}
