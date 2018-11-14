package com.github.myetl.flow.core.parser.core;


import com.github.myetl.flow.core.exception.SqlParseException;
import com.github.myetl.flow.core.parser.SQL;
import com.github.myetl.flow.core.parser.SqlTree;

/**
 * 不同类型sql解析器
 */
public interface IParser {

    boolean accept(String sql);

    SQL parse(String sql, SqlTree sqlTree) throws SqlParseException;
}
