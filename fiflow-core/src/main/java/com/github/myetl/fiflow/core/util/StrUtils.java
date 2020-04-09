package com.github.myetl.fiflow.core.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.Optional;

public class StrUtils {

    public static String toString(Optional<?> optional) {
        if (!optional.isPresent()) {
            return StringUtils.EMPTY;
        }
        return Objects.toString(optional.get());
    }
}
