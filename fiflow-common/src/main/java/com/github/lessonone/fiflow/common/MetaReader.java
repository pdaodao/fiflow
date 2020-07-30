package com.github.lessonone.fiflow.common;

import com.github.lessonone.fiflow.common.base.TableInfo;
import com.github.lessonone.fiflow.common.exception.MetaException;

import java.util.List;

public interface MetaReader {

    String info() throws MetaException;

    List<String> listDatabases() throws MetaException;

    List<String> listTables() throws MetaException;

    List<String> listSchemas() throws MetaException;

    TableInfo getTable(String tableName) throws MetaException;
}
