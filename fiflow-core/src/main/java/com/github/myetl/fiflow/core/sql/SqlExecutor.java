package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.frame.FlowContext;
import com.github.myetl.fiflow.core.frame.FlowSession;

import java.util.Optional;

/**
 * 执行不同 类型的 sql
 */
public final class SqlExecutor {
    // 一条待执行的 sql
    private final String sql;
    private final FlowSession flowSession;

    public SqlExecutor(String sql, FlowSession flowSession) {
        this.sql = sql;
        this.flowSession = flowSession;
    }

    public void run(final FlowContext flowContext){
       Optional<SqlCommander> sqlCommander =  SqlCommandParser.parse(sql);
       if(!sqlCommander.isPresent() ){
           flowContext.outMsg("invalid sql : %s", sql);
           return;
       }

       SqlCommander cmd = sqlCommander.get();

       switch (cmd.command){
           case HELP: callHelp(); break;
           case SHOW_CATALOGS: callShowCatalogs(); break;
           case SHOW_DATABASES: callShowDatabases(); break;
           case SHOW_TABLES: callShowTables(); break;
           case SOURCE: callSource(); break;

           case CREATE_TABLE: callCreateTable(cmd.args[0], flowContext); break;
           case SELECT: callSelect(cmd.args[0], flowContext); break;
       }
    }

    private void callCreateTable(final String sql, FlowContext flowContext) {
        flowSession.tEnv.sqlUpdate(sql);

        flowContext.outMsg("Table has been created.");
    }

    private void callSelect(final String sql, FlowContext flowContext) {

    }

    private void callHelp(){

    }

    private void callShowCatalogs(){

    }

    private void callShowDatabases(){

    }

    private void callShowTables(){

    }

    private void callSource(){

    }

}
