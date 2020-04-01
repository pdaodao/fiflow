package com.github.myetl.fiflow.core.sql;

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.regex.Matcher;

/**
 * 把 sql 解析为特定的 SqlCommand
 */
public final class SqlCommandParser {

    /**
     * 把sql 解析为  SqlCommander
     * @param sql
     * @return
     */
    public static Optional<SqlCommander> parse(String sql) {
        if(StringUtils.isEmpty(sql)) return Optional.empty();
        sql = sql.trim();


        for(final SqlCommander.SqlCommand cmd: SqlCommander.SqlCommand.values()){
           final Matcher matcher = cmd.pattern.matcher(sql);
           if(matcher.matches()){
               final String[] groups = new String[matcher.groupCount()];
               for(int i = 0; i < groups.length; i++){
                   groups[i] = matcher.group(i + 1);
               }
               return cmd.argExtractFun.apply(groups)
                       .map((args) -> new SqlCommander(cmd, args));
           }
        }
        return Optional.empty();
    }
}
