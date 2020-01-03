package com.github.myetl.flow.core.utils;

import com.github.myetl.flow.core.frame.ParsedTableList;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlParserUtils {

    public static SqlParser.Config MYSQL_LEX_CONFIG =
            SqlParser
                    .configBuilder()
                    .setLex(Lex.MYSQL)
                    .build();

    private static SqlNode parse(String sql) throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(sql, MYSQL_LEX_CONFIG);
        SqlNode sqlNode = sqlParser.parseStmt();
        return sqlNode;
    }

    protected static boolean getTableName(SqlNodeList fields,
                                       SqlNode fromNode,
                                       SqlNode where,
                                        ParsedTableList.ReadWriteModel readWriteModel,
                                       ParsedTableList parsedTableList){
        if(fromNode instanceof SqlIdentifier){
            SqlIdentifier sqlIdentifier = (SqlIdentifier) fromNode;
            for (String s : sqlIdentifier.names.asList()) {
                ParsedTableList.ParsedTable table = new ParsedTableList.ParsedTable(s, readWriteModel, where);
                if(fields != null){
                    fields.getList().forEach(field -> {
                        table.addField(field.toString());
                    });
                }
                parsedTableList.add(table);
            }
            return true;
        }
        return false;
    }

    public static ParsedTableList parseTables(SqlNode sqlNode,
                                              ParsedTableList.ReadWriteModel readWriteModel,
                                              ParsedTableList parsedTableList) {
        if (sqlNode == null) return parsedTableList;

        if (sqlNode instanceof SqlIdentifier) {
            getTableName(null, sqlNode, null, readWriteModel, parsedTableList);
            return parsedTableList;
        } else if (sqlNode instanceof SqlSelect) {
            readWriteModel = ParsedTableList.ReadWriteModel.Source;
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            SqlNode fromNode = sqlSelect.getFrom();
            SqlNode where = sqlSelect.getWhere();

            boolean processed = getTableName(sqlSelect.getSelectList(), fromNode, where,
                    readWriteModel, parsedTableList);
            if(processed) return parsedTableList;

            parseTables(fromNode, readWriteModel, parsedTableList);
            return parsedTableList;
        } else if (sqlNode instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin) sqlNode;
            parseTables(sqlJoin.getLeft(), ParsedTableList.ReadWriteModel.Source, parsedTableList);
            parseTables(sqlJoin.getRight(), ParsedTableList.ReadWriteModel.Source, parsedTableList);
            return parsedTableList;
        } else if (sqlNode instanceof SqlInsert) {
            SqlInsert sqlInsert = (SqlInsert) sqlNode;
            parseTables(sqlInsert.getSource(), ParsedTableList.ReadWriteModel.Source, parsedTableList);

            SqlNode target = sqlInsert.getTargetTable();
            boolean targetProcessed = getTableName(sqlInsert.getTargetColumnList(), target, null,
                    ParsedTableList.ReadWriteModel.Sink, parsedTableList);
            if(!targetProcessed){
                parseTables(sqlInsert.getTargetTable(), ParsedTableList.ReadWriteModel.Sink, parsedTableList);
            }
            return parsedTableList;
        }

        return parsedTableList;
    }


    /**
     * 获取 sql 语句中的数据表  select ...  , insert ...
     * @param sql
     * @return
     * @throws SqlParseException
     */
    public static ParsedTableList getTables(String sql) throws SqlParseException {
        SqlNode sqlNode = parse(sql);
        if (sqlNode == null) return null;

        ParsedTableList parsedTableList = new ParsedTableList();

        parseTables(sqlNode, null, parsedTableList);

        return parsedTableList;
    }

}
