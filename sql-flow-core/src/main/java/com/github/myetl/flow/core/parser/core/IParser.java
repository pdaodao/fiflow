package com.github.myetl.flow.core.parser.core;


import com.github.myetl.flow.core.parser.SqlParseException;
import com.github.myetl.flow.core.parser.SqlTree;

/**
 * 不同类型sql解析器
 */
public interface IParser {

    boolean accept(String sql);

    void parse(String sql, SqlTree sqlTree) throws SqlParseException;
}
