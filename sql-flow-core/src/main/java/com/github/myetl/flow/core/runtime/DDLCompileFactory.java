package com.github.myetl.flow.core.runtime;


import com.github.myetl.flow.core.exception.SqlCompileException;
import com.github.myetl.flow.core.parser.DDL;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * source and sink factory
 */
public class DDLCompileFactory {

    private static Map<String, DDLToFlinkCompiler> sourceCompileMap = new ConcurrentHashMap<>();

    private static Map<String, DDLToFlinkCompiler> sinkCompileMap = new ConcurrentHashMap<>();

    static {
        ServiceLoader<DDLToFlinkCompiler> loaders =
                ServiceLoader.load(DDLToFlinkCompiler.class);
        loaders.forEach(t -> registerCompiler(t));
    }

    public static void registerCompiler(DDLToFlinkCompiler ddlToFlinkCompiler) {
        if (ddlToFlinkCompiler == null || StringUtils.isEmpty(ddlToFlinkCompiler.type())) return;

        if (ddlToFlinkCompiler.supportSource())
            sourceCompileMap.put(ddlToFlinkCompiler.type().toLowerCase(), ddlToFlinkCompiler);

        if (ddlToFlinkCompiler.supportSink())
            sinkCompileMap.put(ddlToFlinkCompiler.type().toLowerCase(), ddlToFlinkCompiler);

    }

    private static String getType(DDL ddl) {
        return ddl.getType().toLowerCase();
    }

    public static DDLToFlinkCompiler getCompiler(DDL ddl) {
        DDLToFlinkCompiler compiler = sourceCompileMap.get(getType(ddl));
        if (compiler == null)
            compiler = sinkCompileMap.get(getType(ddl));
        return compiler;
    }

    public static TableSource getTableSource(DDL ddl, RowTypeInfo rowTypeInfo, ExecutionConfig executionConfig) throws SqlCompileException {
        DDLToFlinkCompiler compiler = sourceCompileMap.get(getType(ddl));
        if (compiler == null)
            throw new SqlCompileException(String.format("[%s] not support type", ddl.getType()));
        return compiler.buildSource(ddl, rowTypeInfo, executionConfig);
    }

    public static TableSink getTableSink(DDL ddl, RowTypeInfo rowTypeInfo, ExecutionConfig executionConfig) throws SqlCompileException {
        DDLToFlinkCompiler compiler = sinkCompileMap.get(getType(ddl));
        if (compiler == null)
            throw new SqlCompileException(String.format("[%s] not support type", ddl.getType()));

        return compiler.buildSink(ddl, rowTypeInfo, executionConfig);
    }
}
