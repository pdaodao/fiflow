package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.frame.FlowContext;
import com.github.myetl.fiflow.core.frame.FiFlinkSession;

import java.util.Optional;

/**
 * 执行不同 类型的 sql
 */
public final class SqlExecutor {
    // 一条待执行的 sql
    private final String sql;
    private final FiFlinkSession flowSession;

    public SqlExecutor(String sql, FiFlinkSession flowSession) {
        this.sql = sql;
        this.flowSession = flowSession;
    }

    public SqlBuildContext run(final FlowContext flowContext){
       Optional<SqlCommander> sqlCommander =  SqlCommandParser.parse(sql);
       if(!sqlCommander.isPresent() ){
           flowContext.outMsg("invalid sql : %s", sql);
           return new SqlBuildContext(false);
       }

       SqlCommander cmd = sqlCommander.get();

       switch (cmd.command){
           case HELP: return callHelp(flowContext);
           case SHOW_CATALOGS: return callShowCatalogs();
           case SHOW_DATABASES: return callShowDatabases();
           case SHOW_TABLES: return callShowTables();
           case SOURCE: return callSource();

           case CREATE_TABLE: return callCreateTable(cmd.args[0], flowContext);
           case SELECT: return callSelect(cmd.args[0], flowContext);

           case INSERT_INTO:
           case INSERT_OVERWRITE: return callInsertInto(cmd.args[0], flowContext);
       }
       return new SqlBuildContext(false);
    }

    private SqlBuildContext callInsertInto(final String sql, FlowContext flowContext) {

//        this.tbenv.getConfig().getConfiguration()
//                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
//                        Integer.parseInt(context.getLocalProperties().get("parallelism")));
//
//
        flowSession.tEnv.sqlUpdate(sql);

        flowContext.outMsg("insert into.");
        return new SqlBuildContext(true);
    }

    private SqlBuildContext callCreateTable(final String sql, FlowContext flowContext) {
        flowSession.tEnv.sqlUpdate(sql);
        flowContext.outMsg("Table has been created.");
        return new SqlBuildContext(true);
    }

    private SqlBuildContext callSelect(final String sql, FlowContext flowContext) {
        return new SqlBuildContext(true);
    }

    private SqlBuildContext callHelp(FlowContext flowContext){
        flowContext.outMsg("fiflow run flink sql");
        return new SqlBuildContext(false);
    }

    private SqlBuildContext callShowCatalogs(){
        return new SqlBuildContext(false);
    }

    private SqlBuildContext callShowDatabases(){
        return new SqlBuildContext(false);
    }

    private SqlBuildContext callShowTables(){
        return new SqlBuildContext(false);
    }

    private SqlBuildContext callSource(){
        return new SqlBuildContext(false);
    }

}
