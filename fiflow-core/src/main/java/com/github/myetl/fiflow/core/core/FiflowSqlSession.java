package com.github.myetl.fiflow.core.core;

import com.github.myetl.fiflow.core.frame.SessionConfig;
import com.github.myetl.fiflow.core.sql.SqlCommandBuilder;
import com.github.myetl.fiflow.core.sql.SqlCommandParser;
import com.github.myetl.fiflow.core.sql.SqlCommander;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;

import java.util.List;
import java.util.Optional;

public class FiflowSqlSession extends FiflowSession {

    public FiflowSqlSession(String id, SessionConfig sessionConfig) {
        super(id, sessionConfig);
    }


    /**
     * 把 sqlText 转换成 flink 中的 sql 操作 这里不执行execution
     * @param sqlText  多行以;分隔的sql语句
     */
    @Override
    void sql(String sqlText) {
        List<String> sqls = SqlSplitUtil.split(sqlText);
        for(String sql: sqls) {
            try{
                SqlCommandBuilder.build(sql, this);
            }catch (Exception e){

            }
        }

    }

    @Override
    void close() {

    }
}
