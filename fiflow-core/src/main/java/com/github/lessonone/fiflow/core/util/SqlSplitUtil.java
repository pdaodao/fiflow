package com.github.lessonone.fiflow.core.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlSplitUtil {

    public static final char SQL_DELIMITER = ';';

    public static String trim(String sql) {
        sql = sql.replaceAll("--.*", "")
                .replaceAll("##.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();
        return sql;
    }


    /**
     * 获取 insert into t1(f1,f2,...) 中的表名 t1
     *
     * @param sql
     * @return
     */
    public static String getInsertIntoTableName(String sql) {
        String t = sql.replaceAll("(?i)insert\\s+into\\s+", "").trim();
        t = SqlSplitUtil.getUntil(t, ' ', '(');
        return t;
    }

    /**
     * 获取 create table t1( 中的表名 t1
     *
     * @param sql
     * @return
     */
    public static String getCreateTableName(String sql) {
        String t = sql.replaceAll("(?i)CREATE\\s+TABLE\\s+", "").trim();
        t = getUntil(t, '(', ' ');
        return t;
    }


    /**
     * 遇到 a 或者 b 截取前面的
     *
     * @param sql
     * @param a
     * @param b
     * @return
     */
    public static String getUntil(String sql, char a, char b) {
        if (StringUtils.isBlank(sql)) return StringUtils.EMPTY;
        sql = sql.trim();
        char[] arr = sql.toCharArray();
        StringBuilder sb = new StringBuilder();

        for (char ch : arr) {
            if (ch == a || ch == b) {
                break;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    /**
     * 以; 把 sql 分隔成多行, 忽略 -- 开始的注释
     *
     * @param sqls
     * @return
     */
    public static List<String> split(String sqls) {
        sqls = trim(sqls);
        return splitIgnoreQuota(sqls, SQL_DELIMITER);
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
        if (StringUtils.isEmpty(str)) return tokensList;

        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder();

        boolean isCommet = false;
        final char[] charArray = str.toCharArray();
        final int charSize = charArray.length;
        int index = -1;
        for (char c : charArray) {
            index++;
            if ('-' == c && index < charSize - 1 && charArray[index + 1] == '-') {
                isCommet = true;
            } else if ('\n' == c) {
                b.append(" ");
                if (isCommet) {
                    if (b.length() > 1)
                        tokensList.add(b.toString());
                    b = new StringBuilder();
                    isCommet = false;
                }
                continue;
            }

            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else {
                    if (b.length() > 1)
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

        if (b.length() > 1)
            tokensList.add(b.toString());

        return tokensList.stream().filter(t -> StringUtils.isNotBlank(t)).collect(Collectors.toList());
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
