package com.github.lessonone.fiflow.common.utils;

public class StrUtil {
    public static final char UNDERLINE = '_';

    /**
     * to underline  HelloWorld -> hello_world
     * @param str
     * @return
     */
    public static String toUnderlineCase(String str) {
        return toSymbolCase(str, UNDERLINE);
    }


    /**
     * 将驼峰式命名的字符串转换为使用符号连接方式
     * @param str
     * @param symbol
     * @return
     */
    public static String toSymbolCase(String str, char symbol) {
        if (str == null) {
            return null;
        }

        final int length = str.length();
        final StringBuilder sb = new StringBuilder();
        char c;
        for (int i = 0; i < length; i++) {
            c = str.charAt(i);
            final Character preChar = (i > 0) ? str.charAt(i - 1) : null;
            if (Character.isUpperCase(c)) {
                final Character nextChar = (i < str.length() - 1) ? str.charAt(i + 1) : null;
                if (null != preChar && Character.isUpperCase(preChar)) {
                    sb.append(c);
                } else if (null != nextChar && Character.isUpperCase(nextChar)) {
                    if (null != preChar && symbol != preChar) {
                        sb.append(symbol);
                    }
                    sb.append(c);
                } else {
                    if (null != preChar && symbol != preChar) {
                        sb.append(symbol);
                    }
                    sb.append(Character.toLowerCase(c));
                }
            } else {
                if (sb.length() > 0 && Character.isUpperCase(sb.charAt(sb.length() - 1)) && symbol != c) {
                    sb.append(symbol);
                }
                // 小写或符号
                sb.append(c);
            }
        }
        return sb.toString();
    }


    /**
     * to camel case
     * @param name
     * @return
     */
    public static String toCamelCase(String name) {
        if (null == name) {
            return null;
        }
        if (name.contains("_")) {
            final StringBuilder sb = new StringBuilder(name.length());
            boolean upperCase = false;
            for (int i = 0; i < name.length(); i++) {
                char c = name.charAt(i);

                if (c == UNDERLINE) {
                    upperCase = true;
                } else if (upperCase) {
                    sb.append(Character.toUpperCase(c));
                    upperCase = false;
                } else {
                    sb.append(Character.toLowerCase(c));
                }
            }
            return sb.toString();
        } else {
            return name;
        }
    }


}
