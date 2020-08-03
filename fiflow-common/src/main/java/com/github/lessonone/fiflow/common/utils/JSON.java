package com.github.lessonone.fiflow.common.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Type;

public class JSON {
    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T toPojo(String content, Class<T> valueType) throws JsonProcessingException {
        return objectMapper.readValue(content, valueType);
    }

    public static <T> T toPojo(String content, Type valueType) throws JsonProcessingException {
        JavaType javaType = objectMapper.getTypeFactory().constructType(valueType);
        return objectMapper.readValue(content, javaType);
    }


    public static String toString(Object obj) throws JsonProcessingException {
        if (obj == null) return null;
        return objectMapper.writeValueAsString(obj);
    }
}
