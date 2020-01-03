package com.github.myetl.flow.jdbc;

import com.github.myetl.flow.core.connect.JdbcProperties;
import com.github.myetl.flow.core.exception.SqlFlowMetaReaderException;
import com.github.myetl.flow.core.frame.TableMetaReader;
import com.github.myetl.flow.core.meta.BasicFieldTypeConverter;
import com.github.myetl.flow.core.meta.DBTable;
import com.github.myetl.flow.core.meta.DBTableField;
import com.github.myetl.flow.core.meta.DbFieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * jdbc 数据表元信息 读取
 */
public class JdbcMetaReader implements TableMetaReader {
    private static final String[] TABLES_ONLY = {"TABLE"};

    private final JdbcProperties jdbcProperties;
    private Connection connection = null;

    public JdbcMetaReader(JdbcProperties jdbcProperties) {
        this.jdbcProperties = jdbcProperties;
    }

    public Connection getConnection() throws SqlFlowMetaReaderException {
        if (connection != null)
            return connection;

        long t1 = System.currentTimeMillis();

        try {
            Class.forName(jdbcProperties.getDriverName());
        } catch (ClassNotFoundException e) {
            throw new SqlFlowMetaReaderException(e.getMessage(), e);
        }
        Properties properties = new Properties();
        if (StringUtils.isNotEmpty(jdbcProperties.getUsername()) && StringUtils.isNotEmpty(jdbcProperties.getPassword())) {
            properties.put("user", jdbcProperties.getUsername());
            properties.put("password", jdbcProperties.getPassword());
        }
        try {
            connection = DriverManager.getConnection(jdbcProperties.getDbURL(), properties);
        } catch (SQLException e) {
            connection = null;
            throw new SqlFlowMetaReaderException("connection is null:" + jdbcProperties.getDbURL());
        }

        long t2 = System.currentTimeMillis();

        System.out.println("get connection time: " + (t2 - t1));

        return connection;
    }

    public String catalog() {
        return jdbcProperties.getDbName();
    }

    public String schemaPattern() {
        if (StringUtils.isEmpty(jdbcProperties.getUsername())) {
            return null;
        }
        return jdbcProperties.getUsername();
    }

    @Override
    public synchronized List<DBTable> list() throws SqlFlowMetaReaderException {
        List<DBTable> tables = new ArrayList<>();
        Connection connection = getConnection();
        try {
            DatabaseMetaData dbMeta = connection.getMetaData();
            try (ResultSet rs = dbMeta.getTables(catalog(), schemaPattern(), "%", TABLES_ONLY)) {
                while (rs.next()) {
                    DBTable dataTable = new DBTable(rs.getString("TABLE_NAME"));
                    String remark = rs.getString("REMARKS");
                    dataTable.setComment(remark);
                    tables.add(dataTable);
                }
            }
        } catch (SQLException e1) {
            close();
            throw new SqlFlowMetaReaderException(e1.getMessage(), e1);
        }
        return tables;
    }


    protected DBTableField parseColumnInfo(ResultSet rs) throws SQLException, SqlFlowMetaReaderException {
        String name = rs.getString("COLUMN_NAME");
        String typeName = rs.getString("TYPE_NAME");
        String remarks = rs.getString("REMARKS");
        Integer columnSize = rs.getInt("COLUMN_SIZE");
        Integer columnDigits = rs.getInt("DECIMAL_DIGITS");

        DbFieldType dbFieldType = BasicFieldTypeConverter.convert(typeName, columnSize, columnDigits);

        DBTableField column = new DBTableField(name, dbFieldType);
        column.setComment(remarks);

        return column;
    }


    @Override
    public synchronized List<DBTableField> fields(String tableName) throws SqlFlowMetaReaderException {
        List<DBTableField> columnList = new ArrayList<>();
        Connection connection = getConnection();
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            try (ResultSet rs = databaseMetaData.getColumns(catalog(), schemaPattern(), tableName, null)) {
                while (rs.next()) {
                    columnList.add(parseColumnInfo(rs));
                }
            }
        } catch (SQLException e) {
            close();
            throw new SqlFlowMetaReaderException(e.getMessage(), e);
        }

        return columnList;
    }

    @Override
    public void close() {
        if (connection != null) {
            System.out.println("close connection ... ");
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
