package com.github.lessonone.fiflow.common.exception;

import org.apache.flink.table.catalog.exceptions.CatalogException;

public class AutoMetaNotSupportException extends CatalogException {
    public AutoMetaNotSupportException(String message) {
        super(message);
    }
}
