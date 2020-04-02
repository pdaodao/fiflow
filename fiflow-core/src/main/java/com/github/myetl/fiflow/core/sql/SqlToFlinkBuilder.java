package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.util.JarUtils;
import com.github.myetl.fiflow.core.util.SqlSplitUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Optional;

/**
 * sql command 转化为 flink 的操作
 */
public class SqlToFlinkBuilder {


    /**
     * sql 和 flink 交互
     *
     * @param sql              单条 sql 或者 命令
     * @param fiflowSqlSession
     */
    public static SqlBuildResult build(String sql, FiflowSqlSession fiflowSqlSession) throws Exception {
        Optional<SqlCommander> commander = SqlCommandParser.parse(sql);
        if (!commander.isPresent()) {
            throw new IllegalArgumentException("invalid sql: " + sql);
        }
        SqlCommander cmd = commander.get();
        switch (cmd.command) {
            case HELP:
                return callHelp();
            case FLINK:
                return callFlink();
            case PARALLELISM:
                return callParallelism(cmd.args[0], fiflowSqlSession);
            case JAR:
                return callAddJar(cmd.args[0], fiflowSqlSession);

            case CREATE_TABLE:
                return callCreateTable(cmd.args[0], fiflowSqlSession);
            case INSERT_INTO:
                return callInsertInto(cmd.args[0], fiflowSqlSession);

        }

        return null;

//        FlinkUserCodeClassLoaders.parentFirst(,  .getClassLoader());

    }

    /**
     * 添加依赖 jar
     * @param jars
     * @param session
     * @return
     */
    private static SqlBuildResult callAddJar(final String jars, FiflowSqlSession session) {
        SqlBuildResult result = new SqlBuildResult(BuildLevel.Set);
        if(StringUtils.isEmpty(jars)){
            result.addMsg("jar is empty");
        }
        String[] arrs = jars.split(",|;");
        try{
            List<URL> urls = JarUtils.jars(arrs);
            for(URL url: urls){
                result.addMsg("add classpath jar "+url.getPath());
            }
            for(String jarName : arrs){
                session.addJar(jarName);
            }
        }catch (Exception e){
            SqlBuildResult error = new SqlBuildResult(BuildLevel.Error);
            error.addMsg("add jar "+jars);
            error.addMsg(e.getMessage());
            result = result.merge(error);
        }
        return result;
    }

    /**
     * 设置任务的并发数
     * @param p
     * @param session
     * @return
     */
    private static SqlBuildResult callParallelism(final String p, FiflowSqlSession session) {
        SqlBuildResult result = new SqlBuildResult(BuildLevel.Set);
        result.addMsg("set parallelism " + p);
        Integer pp = Integer.parseInt(p);

        session.tEnv.getConfig().getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, pp);

        return result;
    }

    /**
     * insert into t1(f1,f2,...) select f1, f2, ... from t2 where ...
     *
     * @param sql
     * @param session
     * @return
     */
    private static SqlBuildResult callInsertInto(final String sql, FiflowSqlSession session) {
        SqlBuildResult result = new SqlBuildResult(BuildLevel.Insert);
        session.tEnv.sqlUpdate(sql);

        result.addMsg("prepare insert into " + SqlSplitUtil.getInsertIntoTableName(sql));
        return result;
    }

    /**
     * create table t1( ) with ( )
     *
     * @param sql
     * @param session
     * @return
     */
    private static SqlBuildResult callCreateTable(final String sql, FiflowSqlSession session) {
        SqlBuildResult sqlBuildResult = new SqlBuildResult(BuildLevel.Create);
        session.tEnv.sqlUpdate(sql);
        sqlBuildResult.addMsg("create table " + SqlSplitUtil.getCreateTableName(sql) + " ok");
        return sqlBuildResult;
    }


    /**
     * 帮助信息
     *
     * @return
     */
    private static SqlBuildResult callHelp() {
        SqlBuildResult result = new SqlBuildResult(BuildLevel.Show);
        result.addMsg("help message");

        TableData rowSet = TableData.instance();
        rowSet.addHeads("sql command", "description");
        List<String> lines;
        InputStream inputStream = null;
        try {
            inputStream = SqlToFlinkBuilder.class.getClassLoader().getResourceAsStream("sqlhelp.txt");
            lines = IOUtils.readLines(inputStream);
            for (String line : lines) {
                if(StringUtils.isBlank(line) || !line.contains(";")) continue;
                int index = line.indexOf(";");
                String sql = line.substring(0, index);
                String desc = line.substring(index + 1);
                rowSet.addRow(TableRow.of(sql, desc));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        result.setTable(rowSet);
        return result;
    }

    /**
     * 设置 flink 集群
     */
    private static SqlBuildResult callFlink() {
        return null;
    }
}
