package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import java.util.Optional;

/**
 * sql command 转化为 flink 的操作
 */
public class SqlCommandBuilder {


    /**
     * sql 和 flink 交互
     * @param sql 单条 sql 或者 命令
     * @param fiflowSqlSession
     */
    public static void build(String sql, FiflowSqlSession fiflowSqlSession) throws Exception{
        Optional<SqlCommander> commander = SqlCommandParser.parse(sql);
        if(!commander.isPresent()){
            throw new IllegalArgumentException("invalid sql: "+sql);
        }
        SqlCommander cmd = commander.get();
        switch (cmd.command){
            case FLINK: callFlink();
        }


//        FlinkUserCodeClassLoaders.parentFirst(,  .getClassLoader());

    }

    /**
     * 设置 flink 集群
     */
    private static void callFlink() {

    }
}
