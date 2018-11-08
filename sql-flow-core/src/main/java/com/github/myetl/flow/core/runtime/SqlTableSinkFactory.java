package com.github.myetl.flow.core.runtime;


import com.github.myetl.flow.core.parser.DDL;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.TableSink;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableSink
 */
public class SqlTableSinkFactory {

    private static Map<String, DDLToFlinkCompiler> compilerMap = new ConcurrentHashMap<>();

    public static void registerCompiler(DDLToFlinkCompiler ddlToFlinkCompiler) {
        if (ddlToFlinkCompiler != null && ddlToFlinkCompiler.supportSink()) {
            compilerMap.put(ddlToFlinkCompiler.type().toLowerCase(), ddlToFlinkCompiler);
        }
    }


    public static TableSink getTableSink(DDL ddl, TableEnvironment env) throws SqlCompileException {
        DDLToFlinkCompiler compiler = compilerMap.get(ddl.getType().toLowerCase());
        if (compiler == null)
            throw new SqlCompileException(String.format("[%s] not support type", ddl.getType()));


        return compiler.buildSink(ddl, env);
    }
}
