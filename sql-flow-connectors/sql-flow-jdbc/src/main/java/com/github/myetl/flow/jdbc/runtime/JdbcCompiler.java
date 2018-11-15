package com.github.myetl.flow.jdbc.runtime;

import com.github.myetl.flow.core.exception.SqlCompileException;
import com.github.myetl.flow.core.parser.DDL;
import com.github.myetl.flow.core.runtime.DDLToFlinkCompiler;
import com.github.myetl.flow.core.runtime.InputFormatTableSource;
import com.github.myetl.flow.core.runtime.OutputFormatTableSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * jdbc
 */
public class JdbcCompiler implements DDLToFlinkCompiler {
    @Override
    public String type() {
        return "jdbc";
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @Override
    public boolean supportSource() {
        return true;
    }

    @Override
    public boolean supportSink() {
        return true;
    }

    @Override
    public TableSource buildSource(DDL ddl, RowTypeInfo rowTypeInfo, ExecutionConfig executionConfig) throws SqlCompileException {
        String sql = ddl.getPropertyAsString("sql");
        if(StringUtils.isEmpty(sql)){
            String table = ddl.getPropertyAsString("table");
            if(StringUtils.isEmpty(table))
                table = ddl.getTableName();

            List<String> fields = ddl.getFields().stream().map(t -> t.getName()).collect(Collectors.toList());
            sql = "SELECT "+StringUtils.join(fields, ",")+ " FROM "+table;
        }

        String driver = ddl.getPropertyAsString("driver");
        String url = ddl.getPropertyAsString("url");
        String username = ddl.getPropertyAsString("username");
        String password = ddl.getPropertyAsString("password");

        try{
            JdbcInputFormat jdbcInputFormat = JdbcInputFormat.builder(rowTypeInfo)
                    .setDriver(driver)
                    .setQuery(sql)
                    .setUrl(url)
                    .setUsername(username)
                    .setPassword(password)
                    .build();

            InputFormatTableSource source = new InputFormatTableSource(jdbcInputFormat);
            return source;
        }catch (Exception e){
            throw new SqlCompileException(e);
        }

    }

    @Override
    public TableSink buildSink(DDL ddl, RowTypeInfo rowTypeInfo, ExecutionConfig executionConfig) throws SqlCompileException {
        String driver = ddl.getPropertyAsString("driver");
        String url = ddl.getPropertyAsString("url");
        String username = ddl.getPropertyAsString("username");
        String password = ddl.getPropertyAsString("password");

        String sql = ddl.getPropertyAsString("sql");
        if(StringUtils.isEmpty(sql)){
            String table = ddl.getPropertyAsString("table");
            if(StringUtils.isEmpty(table))
                table = ddl.getTableName();

            List<String> fields = ddl.getFields().stream().map(t -> t.getName()).collect(Collectors.toList());

            List<String> vs = fields.stream().map(t -> "?").collect(Collectors.toList());

            sql = "INSERT INTO  "+table+"("+StringUtils.join(fields, ",")+ ") VALUES ( "+StringUtils.join(vs, ",")+")";
        }

        try{
            JdbcOutputFormat jdbcInputFormat = JdbcOutputFormat.builder(rowTypeInfo)
                    .setInsert(sql)
                    .setDriver(driver)
                    .setUrl(url)
                    .setUsername(username)
                    .setPassword(password)
                    .build();

            OutputFormatTableSink sink = new OutputFormatTableSink(jdbcInputFormat);
            return sink;
        }catch (Exception e){
            throw new SqlCompileException(e);
        }
    }
}
