package com.github.lessonone.fiflow.common.base;

public class DbInfo {
    private String url;
    private String driverClassName;
    private String username;
    private String password;

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
}
