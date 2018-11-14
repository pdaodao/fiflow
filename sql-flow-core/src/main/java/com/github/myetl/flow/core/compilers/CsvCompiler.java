package com.github.myetl.flow.core.compilers;

import com.github.myetl.flow.core.parser.DDL;
import com.github.myetl.flow.core.runtime.DDLToFlinkCompiler;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

import java.io.File;

/**
 * read data from csv file
 */
public class CsvCompiler implements DDLToFlinkCompiler {

    // parameter's key : file path
    private static String Path = "path";

    @Override
    public String type() {
        return "csv";
    }

    @Override
    public boolean isStreaming() {
        return true;
    }

    @Override
    public boolean supportSource() {
        return true;
    }

    @Override
    public boolean supportSink() {
        return true;
    }


    private String getFullPath(String path) {
        if (!path.startsWith(File.separator)) {
            path = System.getProperty("user.dir") + File.separator + path;
        }
        return path;
    }

    @Override
    public TableSource buildSource(DDL ddl, RowTypeInfo rowTypeInfo) {
        String path = (String) ddl.getProps().get(Path);
        path = getFullPath(path);
        TableSource csvSource = new CsvTableSource(path, rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
        return csvSource;
    }

    @Override
    public TableSink buildSink(DDL ddl, RowTypeInfo rowTypeInfo) {
        String path = (String) ddl.getProps().get(Path);
        path = getFullPath(path);
        TableSink sink = new CsvTableSink(path, "|");
        return sink;
    }
}