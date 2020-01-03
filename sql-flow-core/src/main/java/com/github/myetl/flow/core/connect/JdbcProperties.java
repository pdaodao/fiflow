package com.github.myetl.flow.core.connect;

import com.github.myetl.flow.core.TableMetaStore;
import com.github.myetl.flow.core.meta.DbInfo;
import org.apache.commons.lang3.StringUtils;

/**
 * 数据库连接信息
 */
public class JdbcProperties extends ConnectionProperties {
    private String username;
    private String password;
    private String driverName;
    private String dbURL;
    private String dbName;

    public JdbcProperties() {
        connectorType = ConnectorType.jdbc.toString();
    }

    public static JdbcPropertiesBuilder builder(TableMetaStore dbMetaStore) {
        return new JdbcPropertiesBuilder(dbMetaStore);
    }

    public String getUsername() {
        return username;
    }

    public JdbcProperties setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public JdbcProperties setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDriverName() {
        return driverName;
    }

    public JdbcProperties setDriverName(String driverName) {
        this.driverName = driverName;
        return this;
    }

    public String getDbURL() {
        return dbURL;
    }

    public JdbcProperties setDbURL(String dbURL) {
        this.dbURL = dbURL;
        return this;
    }

    public String getDbName() {
        return dbName;
    }

    public JdbcProperties setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public static class JdbcPropertiesBuilder {
        private final JdbcProperties jdbcProperties;
        private final TableMetaStore dbMetaStore;

        public JdbcPropertiesBuilder(TableMetaStore dbMetaStore) {
            this.dbMetaStore = dbMetaStore;
            this.jdbcProperties = new JdbcProperties();
        }

        public JdbcPropertiesBuilder setUsername(String username) {
            jdbcProperties.username = username;
            return this;
        }

        public JdbcPropertiesBuilder setPassword(String password) {
            jdbcProperties.password = password;
            return this;
        }

        public JdbcPropertiesBuilder setDriverName(String driverName) {
            jdbcProperties.driverName = driverName;
            return this;
        }

        public JdbcPropertiesBuilder setDbURL(String dbURL) {
            jdbcProperties.dbURL = dbURL;
            return this;
        }

        public JdbcPropertiesBuilder setDbName(String dbName) {
            jdbcProperties.dbName = dbName;
            return this;
        }

        public DbInfo build() {
            if (StringUtils.isEmpty(jdbcProperties.dbName)) {
//                 dbInfo.getDbURL().lastIndexOf("/")
            }
            DbInfo dbInfo = new DbInfo();

            return dbInfo;
        }

    }
}
