package com.github.lessonone.fiflow.common.utils;

import com.github.lessonone.fiflow.common.base.Table;
import com.github.lessonone.fiflow.common.base.TableField;
import com.github.lessonone.fiflow.common.entity.BaseEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.springframework.beans.BeanUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

public class DaoUtils {
    public static final String PkColumn = "id";

    public static <T> String getTableName(T entity) {
        return getTableName(entity.getClass());
    }

    public static String getTableName(Class<?> clazz) {
        if (clazz == null) throw new IllegalArgumentException("table name not found class is null");
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("table name not found class for class " + clazz.getName());
        }
        Table table = clazz.getAnnotation(Table.class);
        if (StringUtils.isEmpty(table.value())) {
            throw new IllegalArgumentException("table name is empty for class " + clazz.getName());
        }
        return table.value();
    }


    public static <T extends BaseEntity> Tuple2<Long, Map<String, Object>> entityToMap(T entity) {
        if (entity == null) return null;
        PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(entity.getClass());
        Map<String, Object> rowMap = new LinkedHashMap<>();
        Long id = null;
        for (PropertyDescriptor pd : pds) {
            String column = StrUtil.toUnderlineCase(pd.getName());
            if ("class".equals(column)) continue;
            Object value = null;
            try {
                Field f = ReflectionUtils.findField(entity.getClass(), pd.getName());
                if (f.isAnnotationPresent(TableField.class)) {
                    TableField tableField = f.getAnnotation(TableField.class);
                    if (tableField.ignore()) {
                        continue;
                    }
                    if (StringUtils.isNotEmpty(tableField.name())) {
                        column = tableField.name();
                    }
                }
                f.setAccessible(true);
                value = f.get(entity);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            if (value == null)
                continue;
            if (PkColumn.equals(column)) {
                id = (Long) value;
                continue;
            }
            rowMap.put(column, value);
        }
        return new Tuple2<Long, Map<String, Object>>(id, rowMap);
    }
}
