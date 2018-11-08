package com.github.myetl.flow.core;


import com.github.myetl.flow.core.parser.DDL;
import com.github.myetl.flow.core.parser.SqlParser;
import com.github.myetl.flow.core.parser.SqlTree;
import com.github.myetl.flow.core.runtime.DDLToFlinkCompiler;
import com.github.myetl.flow.core.runtime.SqlTableSinkFactory;
import com.github.myetl.flow.core.runtime.SqlTableSourceFactory;
import com.github.myetl.flow.core.runtime.SqlTreeCompiler;
import com.github.myetl.flow.core.util.FlinkFieldTypeUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

import java.io.File;

public class Demo1 {

    public static void main(String[] args) throws Exception {


        String sql = "create table person1(\n" +
                " age int,\n" +
                " name varchar\n" +
                ") with (\n" +
                " type='csv',\n" +
                " path='sql-flow-core/src/test/resources/person.csv'\n" +
                ");\n" +
                "\n" +
                "create table person2(\n" +
                " age int,\n" +
                " name varchar\n" +
                ") with (\n" +
                " type='csv',\n" +
                " path='sql-flow-core/src/test/resources/person2'\n" +
                ");\n" +
                "\n" +
                "\n" +
                "INSERT INTO person2\n" +
                "SELECT\n" +
                "  age,\n" +
                "  name\n" +
                "FROM\n" +
                "  person1 where age > 2;";

        DDLToFlinkCompiler ddlToFlinkCompiler = new CsvCompiler();

        SqlTableSourceFactory.registerCompiler(ddlToFlinkCompiler);
        SqlTableSinkFactory.registerCompiler(ddlToFlinkCompiler);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


        SqlTree sqlTree = SqlParser.parseSql(sql);

        SqlTreeCompiler.compile(sqlTree, tableEnv);

        env.execute();
    }

    public static class CsvCompiler implements DDLToFlinkCompiler {

        @Override
        public String type() {
            return "csv";
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
        public TableSource buildSource(DDL ddl, TableEnvironment env) {
            RowTypeInfo rowTypeInfo = FlinkFieldTypeUtil.toFlinkType(ddl);
            String path = (String) ddl.getProps().get("path");
            path = getFullPath(path);
            TableSource csvSource = new CsvTableSource(path, rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
            return csvSource;
        }

        private String getFullPath(String path) {
            if(!path.startsWith(File.separator)){
                path = System.getProperty("user.dir")+File.separator+path;
            }
            return path;
        }

        @Override
        public TableSink buildSink(DDL ddl, TableEnvironment env) {
            String path = (String) ddl.getProps().get("path");
            path = getFullPath(path);
            TableSink sink = new CsvTableSink(path, "|");
            return sink;
        }
    }


}
