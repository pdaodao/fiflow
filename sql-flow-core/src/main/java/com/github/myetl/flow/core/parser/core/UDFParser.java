package com.github.myetl.flow.core.parser.core;


import com.github.myetl.flow.core.exception.SqlParseException;
import com.github.myetl.flow.core.parser.SQL;
import com.github.myetl.flow.core.parser.SqlTree;
import com.github.myetl.flow.core.parser.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * udf function parser
 * CREATE FUNCTION stringLengthUdf AS 'com.hjc.test.blink.sql.udx.StringLengthUdf';
 */
public class UDFParser implements IParser {

    private static final String FuncPatternStr = "(?i)\\s*create\\s+function\\s+(\\S+)\\s+AS\\s+(\\S+)";

    private static final Pattern FuncPattern = Pattern.compile(FuncPatternStr);


    public static UDFParser newInstance() {
        return new UDFParser();
    }


    @Override
    public boolean accept(String sql) {
        return FuncPattern.matcher(sql).find();
    }

    @Override
    public SQL parse(String sql, SqlTree sqlTree) throws SqlParseException {
        Matcher matcher = FuncPattern.matcher(sql);
        if (matcher.find()) {
            String funcName = matcher.group(1).trim();
            String className = matcher.group(2).replaceAll("'", "").trim();

            UDF udf = new UDF(funcName, className);
            sqlTree.addUdf(udf);
            return udf;
        }
        return null;
    }
}
