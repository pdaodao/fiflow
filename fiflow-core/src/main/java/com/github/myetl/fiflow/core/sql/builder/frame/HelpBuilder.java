package com.github.myetl.fiflow.core.sql.builder.frame;

import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.pojo.TableData;
import com.github.myetl.fiflow.core.pojo.TableRow;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.CmdType;
import com.github.myetl.fiflow.core.sql.SqlSessionContext;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * 帮助信息 help
 */
public class HelpBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "HELP";

    public HelpBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "<span style='color:green'>help</span>; show help message";
    }

    @Override
    public BuildLevel buildLevel() {
        return BuildLevel.Show;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        result.addMsg("help message");

        TableData rowSet = TableData.instance();
        rowSet.addHeads("sql command", "description");
        result.setTable(rowSet);

        for (CmdType cmdType : CmdType.values()) {
            String msg = cmdType.cmdBuilder.help();
            if (!StringUtils.isBlank(msg)) {
                String title = msg;
                String desc = "";
                int index = msg.indexOf(";");
                if (index > 0 && index < msg.length() - 3) {
                    title = msg.substring(0, index);
                    desc = msg.substring(index + 1);
                }
                rowSet.addRow(TableRow.of(title, desc));
            }
        }
        return result;
    }
}
