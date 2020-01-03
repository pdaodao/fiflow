package com.github.myetl.flow.jdbc;

import com.github.myetl.flow.core.connect.JdbcProperties;
import com.github.myetl.flow.core.frame.ConnectorBuilder;
import com.github.myetl.flow.core.frame.TableConnectSetting;

import java.util.HashMap;
import java.util.Map;

/**
 * jdbc Source  Sink 构建器
 */
public class JdbcConnectorBuilder implements ConnectorBuilder {
    final JdbcProperties jdbcProperties;

    public JdbcConnectorBuilder(JdbcProperties jdbcProperties) {
        this.jdbcProperties = jdbcProperties;
    }

    protected Map<String, String> connect(TableConnectSetting tableConnectSetting) {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.type", "jdbc");
        properties.put("connector.url", jdbcProperties.getDbURL());
        properties.put("connector.username", jdbcProperties.getUsername());
        properties.put("connector.password", jdbcProperties.getPassword());
        properties.put("connector.table", tableConnectSetting.tableName);
        return properties;
    }

    @Override
    public Map<String, String> buildSourceProperties(TableConnectSetting tableConnectSetting) {
        Map<String, String> properties = connect(tableConnectSetting);
        // 读数据相关配置
        return properties;
    }

    @Override
    public Map<String, String> buildSinkProperties(TableConnectSetting tableConnectSetting) {
        Map<String, String> properties = connect(tableConnectSetting);
        // 写数据相关配置
        return properties;
    }
}
