package com.github.myetl.flow.jdbc.runtime;


import com.github.myetl.flow.core.outputformat.FlowOutputFormat;
import com.github.myetl.flow.core.outputformat.OutputFormatBuilder;
import com.github.myetl.flow.jdbc.utils.DbUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 写数据到 jdbc
 */
public class JdbcOutputFormat extends FlowOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);
    static final int DEFAULT_BATCH_INTERVAL = 1000;

    // 数据库信息
    private String driver;
    private String url;
    private String username;
    private String password;

    private int batchInterval = DEFAULT_BATCH_INTERVAL;

    // insert 语句
    private String insert;

    private Connection dbConn;
    private PreparedStatement upload;

    private int batchCount = 0;

    private JdbcOutputFormat(RowTypeInfo rowTypeInfo) {
        super(rowTypeInfo);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try{
            dbConn = getConnectionForWrite();
            upload = dbConn.prepareStatement(insert);
        }catch (SQLException e){
            throw new IllegalArgumentException("open () failed. ", e);
        }
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        try{
            for(int i = 0; i< record.getArity(); i++){
                upload.setObject(i+1, record.getField(i));
            }
            upload.addBatch();
            batchCount++;
        }catch (SQLException e){
            throw new RuntimeException("jdbc write addBatch failed. ", e);
        }

        if(batchCount >= batchInterval)
            this.flush();
    }

    private void flush(){
        try{
            upload.executeBatch();
            batchCount = 0;
        }catch (SQLException e){
            throw new RuntimeException("Jdbc batch write failed .", e);
        }
    }

    @Override
    public void close() throws IOException {
        if(this.upload != null){
            flush();
        }
        try{
            DbUtil.close(dbConn, upload);
        }catch (SQLException e){
            LOG.error("jdbc write connection could not be closed: "+e.getMessage(), e);
        }
    }

    public Connection getConnectionForWrite() throws SQLException {
        Connection connection = DbUtil.getConnection(driver,
                url,
                username,
                password);
        return connection;
    }



    public static JdbcOutputFormatBuilder builder(RowTypeInfo rowTypeInfo){
        return new JdbcOutputFormatBuilder(rowTypeInfo);
    }


    public static class JdbcOutputFormatBuilder implements OutputFormatBuilder<JdbcOutputFormat> {

        private final JdbcOutputFormat format;


        public JdbcOutputFormatBuilder(RowTypeInfo rowTypeInfo) {
            this.format = new JdbcOutputFormat(rowTypeInfo);
        }

        public JdbcOutputFormatBuilder setDriver(String driver) {
            this.format.driver = driver;
            return this;
        }

        public JdbcOutputFormatBuilder setUrl(String url) {
            this.format.url = url;
            return this;
        }

        public JdbcOutputFormatBuilder setUsername(String username) {
            this.format.username = username;
            return this;
        }

        public JdbcOutputFormatBuilder setInsert(String insert) {
            this.format.insert = insert;
            return this;
        }


        public JdbcOutputFormatBuilder setPassword(String password) {
            this.format.password = password;
            return this;
        }

        public JdbcOutputFormatBuilder setBatchInterval(int batchInterval) {
            format.batchInterval = batchInterval;
            return this;
        }

        @Override
        public JdbcOutputFormat build() {

            Preconditions.checkNotNull(format.driver, "JdbcInputFormat Driver not supplied");
            Preconditions.checkNotNull(format.url, "JdbcInputFormat url not supplied");
            Preconditions.checkNotNull(format.insert, "JdbcInputFormat insert sql not supplied");

            if (format.username == null) {
                LOG.debug("Database Username was not supplied separately.");
            }
            if (format.password == null) {
                LOG.debug("Database Password was not supplied separately.");
            }


            Preconditions.checkArgument(format.batchInterval > 0 && format.batchInterval < 10000,
                    "Jdbc write batchInterval should > 0 and < 10000");

            return format;
        }
    }
}
