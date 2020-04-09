package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * sql command 转化为 flink 的操作
 */
public class SqlToFlinkBuilder {


    /**
     * sql command 转换为 flink 的操作
     *
     * @param sql              单条 sql
     * @param fiflowSqlSession
     * @return
     * @throws Exception
     */
    public static CmdBuildInfo build(String sql, FiflowSqlSession fiflowSqlSession) throws Exception {
        if (StringUtils.isEmpty(sql))
            return new CmdBuildInfo(BuildLevel.None);
        sql = sql.trim();
        for (CmdType cmdType : CmdType.values()) {
            Optional<String[]> accept = cmdType.cmdBuilder.accept(sql);
            if (accept.isPresent()) {
                Cmd cmd = new Cmd(cmdType, accept.get());
                return cmdType.cmdBuilder.build(cmd, fiflowSqlSession);
            }
        }
        throw new IllegalArgumentException("unknown sql type: " + sql);
    }


}
