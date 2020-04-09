package com.github.myetl.fiflow.core.sql.builder.frame;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.pojo.TableData;
import com.github.myetl.fiflow.core.pojo.TableRow;
import com.github.myetl.fiflow.core.sql.*;
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
        return "help; show help message";
    }

    @Override
    public CmdBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Show);
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
