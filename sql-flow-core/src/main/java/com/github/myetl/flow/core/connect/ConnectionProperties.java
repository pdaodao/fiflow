package com.github.myetl.flow.core.connect;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库连接信息
 */
public class ConnectionProperties implements Serializable {
    private static Integer autoId = 0;

    /**
     * 连接类型 每一种连接器都有一个连接类型 jdbc elasticsearch hbase ...
     */
    protected String connectorType;

    public Integer id = null;

    {
        if(id == null) id = autoId++;
    }

    private Map<String, String> properties = new HashMap<>();

    public ConnectionProperties put(String key, String value) {
        this.properties.put(key, value);
        return this;
    }

    public Map<String, String> getProperties() {
        Map<String, String> p = new HashMap<>();
        p.putAll(properties);
        return p;
    }

    public String getProperty(String key) {
        return this.properties.get(key);
    }

    public String getConnectorType() {
        return connectorType;
    }
}
