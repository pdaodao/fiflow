package com.github.myetl.flow.jdbc.runtime;

import com.github.myetl.flow.core.exception.SqlCompileException;
import com.github.myetl.flow.core.parser.DDL;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

/**
 * mysql
 */
public class MysqlCompiler extends JdbcCompiler{

    @Override
    public String type() {
        return "mysql";
    }

    private void addDriver(DDL ddl){
        ddl.getProps().put("driver", "com.mysql.jdbc.Driver");
    }

    @Override
    public TableSink buildSink(DDL ddl, RowTypeInfo rowTypeInfo, ExecutionConfig executionConfig) throws SqlCompileException {
        addDriver(ddl);
        return super.buildSink(ddl, rowTypeInfo, executionConfig);
    }

    @Override
    public TableSource buildSource(DDL ddl, RowTypeInfo rowTypeInfo, ExecutionConfig executionConfig) throws SqlCompileException {
        addDriver(ddl);
        return super.buildSource(ddl, rowTypeInfo, executionConfig);
    }
}
