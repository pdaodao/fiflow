package com.github.lessonone.fiflow.common.base;

import java.sql.SQLException;


@FunctionalInterface
public interface FunctionWithException<T, R> {
    R apply(T t) throws SQLException;
}
