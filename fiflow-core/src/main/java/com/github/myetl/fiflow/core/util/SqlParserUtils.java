package com.github.myetl.fiflow.core.util;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.calcite.CalciteParser;

/**
 * sql 解析
 */
public class SqlParserUtils {
    private static final SqlParser.Config PARSER_CONFIG = SqlParser
            .configBuilder()
            .setParserFactory(FlinkSqlParserImpl.FACTORY)
            .setConformance(FlinkSqlConformance.DEFAULT)
            .setLex(Lex.JAVA)
            .build();
    private static final CalciteParser parser = new CalciteParser(PARSER_CONFIG);

    public static SqlNode parse(String sql) throws SqlParseException {
        return parser.parse(sql);
    }
}
