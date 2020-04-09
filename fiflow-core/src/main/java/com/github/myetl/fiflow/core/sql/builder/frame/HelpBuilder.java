package com.github.myetl.fiflow.core.sql.builder.frame;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.pojo.TableData;
import com.github.myetl.fiflow.core.pojo.TableRow;
import com.github.myetl.fiflow.core.sql.*;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

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

        for(CmdType cmdType: CmdType.values()){
            String msg = cmdType.cmdBuilder.help();
            if(!StringUtils.isBlank(msg)){
                String title = msg;
                String desc = "";
                int index = msg.indexOf(";");
                if(index > 0 && index < msg.length() - 3){
                    title = msg.substring(0, index);
                    desc = msg.substring(index + 1);
                }
                rowSet.addRow(TableRow.of(title, desc));
            }
        }

        return result;
    }

    /**
     * 从 sqlhelp.txt 读取帮助信息
     *
     * @param cmd
     * @param session
     * @return
     */
    public CmdBuildInfo build2(Cmd cmd, FiflowSqlSession session) {
        CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Show);
        result.addMsg("help message");

        TableData rowSet = TableData.instance();
        rowSet.addHeads("sql command", "description");
        List<String> lines;
        InputStream inputStream = null;
        try {
            inputStream = SqlToFlinkBuilder.class.getClassLoader().getResourceAsStream("sqlhelp.txt");
            lines = IOUtils.readLines(inputStream);
            for (String line : lines) {
                if (StringUtils.isBlank(line) || !line.contains(";")) continue;
                int index = line.indexOf(";");
                String sql = line.substring(0, index);
                String desc = line.substring(index + 1);
                rowSet.addRow(TableRow.of(sql, desc));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        result.setTable(rowSet);
        return result;
    }
}
