package com.github.myetl.flow.core.util;

import java.util.ArrayList;
import java.util.List;

/**
 * sql 解析过程中使用到的工具方法
 */
public class SqlParseUtil {


    public static String trim(String sql) {
        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();
        return sql;
    }

    /**
     * 以;把sql分割成多行 忽略引号中的;
     *
     * @param str
     * @param delimiter
     * @return
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter) {
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder();
        for (char c : str.toCharArray()) {
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"') {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'') {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else {
                b.append(c);
            }
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    /**
     * 分割字符忽略 分号 和 引号
     *
     * @param str
     * @param delimter
     * @return
     */
    public static String[] splitIgnoreQuotaBrackets(String str, String delimter) {
        String splitPatternStr = delimter + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])(?=(?:[^\"]|\"[^\"]*\")*$)";
        return str.split(splitPatternStr);
    }

}
