package com.github.myetl.fiflow.core.sql.builder.system;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;
import com.github.myetl.fiflow.core.util.StrUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * describe xx
 */
public class DescribeBuilder extends CmdBaseBuilder implements CmdBuilder {
    private static final String pattern = "DESCRIBE\\s+(.*)";

    public DescribeBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "describe xx; show table schema";
    }

    @Override
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        final String tableName = cmd.args[0];

        FlinkBuildInfo result = new FlinkBuildInfo(BuildLevel.Show);

        Table table = session.tEnv.from(tableName);
        if (table == null) {
            result = new FlinkBuildInfo(BuildLevel.Error);
            result.addMsg("table not exist " + tableName);
            return result;
        }

        result.table().addHeads("field", "type", "primary key");

        TableSchema tableSchema = table.getSchema();
        Map<String, String> pkFieldMap = new HashMap<>();

        Optional<UniqueConstraint> pks = tableSchema.getPrimaryKey();
        if (pks.isPresent()) {
            UniqueConstraint pk = pks.get();
            for (String t : pk.getColumns()) {
                pkFieldMap.put(t, pk.getType().name());
            }
        }

        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            String f = StrUtils.toString(tableSchema.getFieldName(i));
            String k = "";
            if (pkFieldMap.containsKey(f)) {
                k = pkFieldMap.get(f);
            }
            result.table().addRow(f, StrUtils.toString(tableSchema.getFieldDataType(i)), k);
        }

        return result;
    }
}
