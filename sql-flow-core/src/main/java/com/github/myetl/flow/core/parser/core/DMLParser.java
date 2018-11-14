package com.github.myetl.flow.core.parser.core;

import com.github.myetl.flow.core.exception.SqlParseException;
import com.github.myetl.flow.core.parser.DML;
import com.github.myetl.flow.core.parser.SQL;
import com.github.myetl.flow.core.parser.SqlTree;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 * INSERT INTO tableName [ (columnName[ , columnName]*) ] queryStatement;
 */
public class DMLParser implements IParser {

    public static DMLParser newInstance() {
        return new DMLParser();
    }

    @Override
    public boolean accept(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert ");
    }

    @Override
    public SQL parse(String sql, SqlTree sqlTree) throws SqlParseException {

        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();
        configBuilder.setQuotedCasing(Casing.UNCHANGED);
        configBuilder.setUnquotedCasing(Casing.UNCHANGED);

        SqlParser sqlParser = SqlParser.create(sql, configBuilder.build());

        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            throw new SqlParseException(e.getMessage(), e);
        }
        if (sqlNode == null) {
            throw new SqlParseException(String.format("dml [%s] parsed SqlNode is null", sql));
        }
        DML dml = new DML(sqlNode.toString());
        parseNode(sqlNode, dml);
        sqlTree.addDml(dml);
        return dml;
    }

    private void parseNode(SqlNode sqlNode, DML dml) throws SqlParseException {
        if (sqlNode == null) return;
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                if (StringUtils.isNotEmpty(dml.getTargetTable())) {
                    throw new SqlParseException(String.format("DDL [%s] insert into table already exists", sqlNode.toString()));
                }
                SqlInsert insertNode = (SqlInsert) sqlNode;
                SqlNode target = insertNode.getTargetTable();
                dml.setTargetTable(target.toString());

                SqlNodeList columns = insertNode.getTargetColumnList();
                if (columns != null && columns.size() > 0) {
                    List<String> names = new ArrayList<>();
                    columns.forEach(f -> {
                        names.add(f.toString());
                    });
                    dml.setTargetFields(names);
                }
                parseNode(insertNode.getSource(), dml);
                break;
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                SqlNode sqlFrom = sqlSelect.getFrom();
                if (sqlFrom.getKind() == SqlKind.IDENTIFIER) {
                    dml.addSourceTable(sqlFrom.toString());
                } else {
                    parseNode(sqlFrom, dml);
                }
                break;
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) sqlNode;
                SqlNode leftNode = sqlJoin.getLeft();
                SqlNode rightNode = sqlJoin.getRight();
                if (leftNode.getKind() == SqlKind.IDENTIFIER) {
                    dml.addSourceTable(leftNode.toString());
                } else {
                    parseNode(leftNode, dml);
                }
                if (rightNode.getKind() == SqlKind.IDENTIFIER) {
                    dml.addSourceTable(rightNode.toString());
                } else {
                    parseNode(rightNode, dml);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                if (identifierNode.getKind() != IDENTIFIER) {
                    parseNode(identifierNode, dml);
                } else {
                    dml.addSourceTable(identifierNode.toString());
                }
                break;
            default:
                break;
        }

    }
}
