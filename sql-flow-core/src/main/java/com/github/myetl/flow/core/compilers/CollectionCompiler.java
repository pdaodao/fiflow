package com.github.myetl.flow.core.compilers;

import com.github.myetl.flow.core.exception.SqlCompileException;
import com.github.myetl.flow.core.inputformat.CollectionInputFormat;
import com.github.myetl.flow.core.outputformat.SystemOutOutputFormat;
import com.github.myetl.flow.core.parser.DDL;
import com.github.myetl.flow.core.runtime.DDLToFlinkCompiler;
import com.github.myetl.flow.core.runtime.InputFormatTableSource;
import com.github.myetl.flow.core.runtime.OutputFormatTableSink;
import com.github.myetl.flow.core.util.SqlParseUtil;
import com.github.myetl.flow.core.util.StringCastUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


/**
 * collection element as source
 */
public class CollectionCompiler implements DDLToFlinkCompiler {

    @Override
    public String type() {
        return "collection";
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
    public TableSource buildSource(DDL ddl, RowTypeInfo rowTypeInfo) throws SqlCompileException {
        String data = ddl.getPropertyAsString("data");
        if (StringUtils.isEmpty(data))
            throw new SqlCompileException("CollectionSource data is empty");
        String[] lines = SqlParseUtil.splitIgnoreQuotaBrackets(data, ";");

        final int size = rowTypeInfo.getArity();

        List<Row> rows = new ArrayList<>(lines.length);
        for (String line : lines) {
            String[] fields = SqlParseUtil.splitIgnoreQuotaBrackets(line, ",");
            if (size != fields.length) {
                throw new SqlCompileException("CollectionSource schema field size and value field size not equal !");
            }
            Row row = new Row(size);
            for (int i = 0; i < size; i++) {
                row.setField(i, StringCastUtil.parse(fields[i], rowTypeInfo.getTypeAt(i)));
            }
            rows.add(row);
        }


        CollectionInputFormat inputFormat = new CollectionInputFormat(rowTypeInfo, rows);
        InputFormatTableSource inputformatTableSource = new InputFormatTableSource(inputFormat);
        return inputformatTableSource;
    }

    @Override
    public TableSink buildSink(DDL ddl, RowTypeInfo rowTypeInfo) throws SqlCompileException {
        SystemOutOutputFormat outOutputFormat = new SystemOutOutputFormat(rowTypeInfo);
        OutputFormatTableSink sink = new OutputFormatTableSink(outOutputFormat);
        return sink;
    }
}
