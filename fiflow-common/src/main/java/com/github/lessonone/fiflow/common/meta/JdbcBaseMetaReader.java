package com.github.lessonone.fiflow.common.meta;

import com.github.lessonone.fiflow.common.MetaReader;
import com.github.lessonone.fiflow.common.base.DbInfo;
import com.github.lessonone.fiflow.common.base.FunctionWithException;
import com.github.lessonone.fiflow.common.base.TableInfo;
import com.github.lessonone.fiflow.common.exception.MetaException;
import com.github.lessonone.fiflow.common.utils.DbUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class JdbcBaseMetaReader implements MetaReader {
    public static final String[] TABLES_ONLY = {"TABLE"};
    public static final String[] TABLES_AND_VIEWS = {"TABLE", "VIEW"};
    private final DbInfo dbInfo;
    private DataSource dataSource = null;


    public JdbcBaseMetaReader(DbInfo dbInfo) {
        this.dbInfo = dbInfo;
    }

    protected DataSource getDatasource() {
        if (dataSource != null) return dataSource;
        dataSource = DbUtils.createDatasource(dbInfo);
        return dataSource;
    }

    protected Connection getConnection() throws SQLException {
        return getDatasource().getConnection();
    }

    public String getCatalog() {
        return wrap(Connection::getCatalog);
    }

    public String getSchema() {
        return wrap(Connection::getSchema);
    }

    protected <R> R wrap(FunctionWithException<Connection, R> f) throws MetaException {
        try (Connection conn = getConnection()) {
            return f.apply(conn);
        } catch (SQLException e) {
            throw new MetaException(e.getMessage(), e);
        } catch (Exception e2) {
            throw new MetaException(e2.getMessage(), e2);
        }
    }

    @Override
    public String info() throws MetaException {
        return wrap(conn -> {
            DatabaseMetaData meta = conn.getMetaData();
            return meta.getDatabaseProductName() + ":" + meta.getDatabaseProductVersion();
        });
    }

    @Override
    public List<String> listDatabases() throws MetaException {
        return wrap(connection -> {
            List<String> dbs = new ArrayList<>();
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                while (rs.next()) {
                    dbs.add(rs.getString(1));
                }
            }
            return dbs;
        });
    }

    @Override
    public List<String> listSchemas() throws MetaException {
        return wrap(connection -> {
            List<String> dbs = new ArrayList<>();
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                while (rs.next()) {
                    dbs.add(rs.getString(1));
                }
            }
            return dbs;
        });
    }


    @Override
    public List<String> listTables() throws MetaException {
        return wrap(connection -> {
            List<String> tables = new ArrayList<>();
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getTables(getCatalog(), getSchema(), null, TABLES_ONLY)) {
                while (rs.next()) {
                    String name = rs.getString("TABLE_NAME");
                    String remarks = rs.getString("REMARKS");
                    tables.add(name);
                }
            }
            return tables;
        });
    }

    @Override
    public TableInfo getTable(String tableName) throws MetaException {
        return wrap(connection -> {
            TableInfo tableInfo = new TableInfo(tableName);
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getColumns(getCatalog(), getSchema(), tableName, null)) {
                while (rs.next()) {
                    TableInfo.TableColumn column = tableInfo.addColumn(rs.getString("COLUMN_NAME"),
                            rs.getString("TYPE_NAME"), rs.getString("REMARKS"));
                    column.setSize(rs.getInt("COLUMN_SIZE"));
                    column.setDigits(rs.getInt("DECIMAL_DIGITS"));
                    column.setPosition(rs.getInt("ORDINAL_POSITION"));
                    String autoincrement = rs.getString("IS_AUTOINCREMENT");
                    if ("NO".equals(autoincrement)) {
                        column.setAutoincrement(false);
                    } else {
                        column.setAutoincrement(true);
                    }
                    String nullable = rs.getString("IS_NULLABLE");
                    if ("NO".equals(nullable)) {
                        column.setNullable(false);
                    } else {
                        column.setNullable(true);
                    }
                }
            }

            try (ResultSet rs = metaData.getPrimaryKeys(getCatalog(), getSchema(), tableName)) {
                while (rs.next()) {
                    String field = rs.getString("COLUMN_NAME");
                    String pkName = rs.getString("PK_NAME");
                    tableInfo.addPrimaryKey(pkName, field);
                }
            }

            return tableInfo;
        });
    }
}
