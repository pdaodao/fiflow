package com.github.myetl.flow.core.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.math.BigDecimal;
import java.math.BigInteger;

public class StringCastUtil {


    public static Object parse(String str, TypeInformation<?> type) {
        if (StringUtils.isEmpty(str)) return null;
        if (type == null || !type.isBasicType()) {
            throw new IllegalArgumentException("StringCastUtil field type is not basic");
        }

        Class<?> clazz = type.getTypeClass();

        if (clazz == String.class)
            return str;
        if (clazz == Integer.class)
            return Integer.parseInt(str);
        if (clazz == Long.class)
            return Long.parseLong(str);
        if (clazz == Float.class)
            return Float.parseFloat(str);
        if (clazz == Double.class)
            return Double.parseDouble(str);
        if (clazz == Boolean.class)
            return Boolean.parseBoolean(str);
        if (clazz == Short.class)
            return Short.parseShort(str);
        if (clazz == BigInteger.class)
            return new BigInteger(str);
        if (clazz == BigDecimal.class)
            return new BigDecimal(str);


        return str;
    }
}
