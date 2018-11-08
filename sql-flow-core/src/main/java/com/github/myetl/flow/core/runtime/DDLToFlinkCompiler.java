package com.github.myetl.flow.core.runtime;

import com.github.myetl.flow.core.parser.DDL;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

/**
 * sql ddl to flink
 */
public interface DDLToFlinkCompiler {

    String type();

    boolean isStreaming();

    boolean supportSource();

    boolean supportSink();

    TableSource buildSource(DDL ddl, TableEnvironment env);

    TableSink buildSink(DDL ddl, TableEnvironment env);
}
