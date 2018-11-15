package com.github.myetl.flow.jdbc.runtime;

import com.github.myetl.flow.core.inputformat.FlowInputFormat;
import com.github.myetl.flow.core.inputformat.InputFormatBuilder;
import com.github.myetl.flow.jdbc.utils.DbUtil;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * jdbc 输入
 */
public class JdbcInputFormat extends FlowInputFormat implements NonParallelInput{

    private static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);

    // 数据库信息
    private String driver;
    private String url;
    private String username;
    private String password;

    // sql 语句
    private String query;

    private int resultSetType;
    private int resultSetConcurrency;

    private int fetchSize;

    private transient Connection dbConn;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private boolean hasNext;

    private JdbcInputFormat(RowTypeInfo rowTypeInfo) {
        super(rowTypeInfo);
    }

    public static JdbcInputFormatBuilder builder(RowTypeInfo rowTypeInfo) {
        return new JdbcInputFormatBuilder(rowTypeInfo);
    }


    @Override
    public void openInputFormat() throws IOException {
        try {
            this.dbConn = this.getConnectionForRead();
            statement = dbConn.prepareStatement(query, resultSetType, resultSetConcurrency);
            if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("open () failed" + e.getMessage(), e);
        }
    }

    @Override
    public void closeInputFormat() throws IOException {
        DbUtil.closeQuietly(dbConn, statement);
    }

    @Override
    public void open(InputSplit split) throws IOException {
        try {
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();

        } catch (SQLException e) {
            throw new IllegalArgumentException("JdbcInputformat execute query failed " + e.getMessage(), e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {

        try {
            if (!hasNext) return null;
            for (int pos = 0; pos < reuse.getArity(); pos++) {
                reuse.setField(pos, resultSet.getObject(pos + 1));
            }
            hasNext = resultSet.next();
            return reuse;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException ne) {
            throw new IOException("Couldn't access resultSet", ne);
        }
    }

    @Override
    public void close() throws IOException {
        DbUtil.closeQuietly(resultSet);
    }

    public Connection getConnectionForRead() throws SQLException {
        Connection connection = DbUtil.getConnection(driver,
                url,
                username,
                password);
        connection.setReadOnly(true);
        return connection;
    }


    public static class JdbcInputFormatBuilder implements InputFormatBuilder<JdbcInputFormat> {

        private final JdbcInputFormat format;


        public JdbcInputFormatBuilder(RowTypeInfo rowTypeInfo) {

            Preconditions.checkNotNull(rowTypeInfo, "JdbcInputFromat rowTypeInfo is null!");

            this.format = new JdbcInputFormat(rowTypeInfo);
            this.format.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
            this.format.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        }

        public JdbcInputFormatBuilder setDriver(String driver) {
            this.format.driver = driver;
            return this;
        }

        public JdbcInputFormatBuilder setUrl(String url) {
            this.format.url = url;
            return this;
        }

        public JdbcInputFormatBuilder setUsername(String username) {
            this.format.username = username;
            return this;
        }

        public JdbcInputFormatBuilder setPassword(String password) {
            this.format.password = password;
            return this;
        }

        public JdbcInputFormatBuilder setQuery(String query) {
            this.format.query = query;
            return this;
        }

        public JdbcInputFormatBuilder setResultSetType(int resultSetType) {
            this.format.resultSetType = resultSetType;
            return this;
        }

        public JdbcInputFormatBuilder setResultSetConcurrency(int resultSetConcurrency) {
            format.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public JdbcInputFormatBuilder setFetchSize(int fetchSize) {
            Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0,
                    "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
            format.fetchSize = fetchSize;
            return this;
        }


        @Override
        public JdbcInputFormat build() {
            Preconditions.checkNotNull(format.driver, "JdbcInputFormat Driver not supplied");
            Preconditions.checkNotNull(format.url, "JdbcInputFormat url not supplied");
            Preconditions.checkNotNull(format.query, "JdbcInputFormat query not supplied");

            if (format.username == null) {
                LOG.debug("Database Username was not supplied separately.");
            }
            if (format.password == null) {
                LOG.debug("Database Password was not supplied separately.");
            }

            return format;
        }
    }

}