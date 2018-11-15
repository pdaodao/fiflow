package com.github.myetl.flow.core.parser;


import com.github.myetl.flow.core.exception.SqlParseException;
import com.github.myetl.flow.core.parser.core.DDLParser;
import com.github.myetl.flow.core.parser.core.DMLParser;
import com.github.myetl.flow.core.parser.core.IParser;
import com.github.myetl.flow.core.parser.core.UDFParser;
import com.github.myetl.flow.core.runtime.DDLCompileFactory;
import com.github.myetl.flow.core.runtime.DDLToFlinkCompiler;
import com.github.myetl.flow.core.util.SqlParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;


/**
 * 解析sql
 */
public class SqlParser {

    private static final char SQL_DELIMITER = ';';

    public static List<IParser> SqlParserList = Lists.newArrayList(
            UDFParser.newInstance(),
            DDLParser.newInstance(),
            DMLParser.newInstance());

    /**
     * 解析 sql
     *
     * @param sql
     * @return
     * @throws SqlParseException
     */
    public static SqlTree parseSql(String sql) throws SqlParseException {
        if (StringUtils.isEmpty(sql))
            throw new SqlParseException("sql is empty!");
        sql = SqlParseUtil.trim(sql);
        List<String> sqls = SqlParseUtil.splitIgnoreQuota(sql, SQL_DELIMITER);

        SqlTree sqlTree = new SqlTree();

        for (String sqlLine : sqls) {
            if (StringUtils.isBlank(sqlLine)) continue;

            boolean hasParser = false;
            for (IParser iParser : SqlParserList) {
                if (!iParser.accept(sqlLine)) {
                    continue;
                }
                hasParser = true;
                SQL sqlBlock = iParser.parse(sqlLine, sqlTree);
                if (sqlBlock != null && sqlBlock instanceof DDL ) {
                    DDLToFlinkCompiler compiler = DDLCompileFactory.getCompiler((DDL) sqlBlock);
                    if (compiler != null&& compiler.isStreaming() ) sqlTree.setIsStream(true);
                }
            }
            if (!hasParser) {
                throw new SqlParseException(String.format("%s:Syntax does not support", sqlLine));
            }
        }


        return sqlTree;
    }
}
