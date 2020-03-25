package com.github.myetl.fiflow.core.util;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

public final class Preconditions {

    public static <T> T checkNotNull(T reference, @Nullable String errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    public static String checkNotEmpty(String reference, @Nullable String errorMessage) {
        if (StringUtils.isEmpty(reference)) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }


}
