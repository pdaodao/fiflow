package com.github.lessonone.fiflow.common.base;

import java.util.LinkedHashMap;
import java.util.Map;

public class DbInfo {
    private String url;
    private String driverClassName;
    private String username;
    private String password;
    private Map<String, String> properties;

    public String getUrl() {
        return url;
    }

    public DbInfo setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public DbInfo setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public DbInfo setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DbInfo setPassword(String password) {
        this.password = password;
        return this;
    }

    public DbInfo addProperty(String key, String value){
        if(this.properties == null)
            this.properties = new LinkedHashMap<>();
        this.properties.put(key, value);
        return this;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
