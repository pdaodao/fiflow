package com.github.myetl.fiflow.core.util;

import com.github.myetl.fiflow.core.sql.builder.SelectBuilder;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
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

    public static SqlNode parseWhere(String sql) throws SqlParseException {
        if (StringUtils.isEmpty(sql)) return null;
        if (SelectBuilder.pattern.matches(sql)) {

        } else {
            sql = sql.trim();
            StringBuilder sb = new StringBuilder();
            sb.append("select * from t1");
            sb.append(" where");

            sb.append(" ");
            sb.append(sql);

            sql = sb.toString();
        }
        return ((SqlSelect) parse(sql)).getWhere();
    }
}
