package com.github.myetl.flow.jdbc;

import com.github.myetl.flow.core.ConnectorFactory;
import com.github.myetl.flow.core.connect.ConnectionProperties;
import com.github.myetl.flow.core.connect.ConnectorType;
import com.github.myetl.flow.core.connect.JdbcProperties;
import com.github.myetl.flow.core.frame.ConnectorBuilder;
import com.github.myetl.flow.core.frame.TableMetaReader;

/**
 * jdbc 连接器 入口
 */
public class JdbcConnectorFactory implements ConnectorFactory {
    @Override
    public boolean canHandle(String connectorType) {
        if (ConnectorType.jdbc.name().equals(connectorType.toLowerCase().trim())) {
            return true;
        }
        return false;
    }

    @Override
    public TableMetaReader metaReader(ConnectionProperties connectionProperties) {
        if (connectionProperties instanceof JdbcProperties) {
            return new JdbcMetaReader((JdbcProperties) connectionProperties);
        }
        return null;
    }

    @Override
    public ConnectorBuilder connectorBuilder(ConnectionProperties connectionProperties) {
        if (connectionProperties instanceof JdbcProperties) {
            return new JdbcConnectorBuilder((JdbcProperties) connectionProperties);
        }
        return null;
    }
}
