package com.github.myetl.flow.jdbc.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * 数据库操作相关工具函数
 * 获取 释放 数据库连接
 */
public final class DbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DbUtil.class);


    private DbUtil() {
    }

    /**
     * 加载数据库驱动类
     * @param clazz
     * @param classLoader
     */
    public synchronized static void forName(String clazz, ClassLoader classLoader) {
        try {
            Class<?> driverClass = classLoader.loadClass(clazz);
            driverClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 加载数据库驱动类
     * @param clazz
     */
    public synchronized static void forName(String clazz) {
        try {
            Class<?> driverClass = Class.forName(clazz);
            driverClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取数据库连接
     * @param driver
     * @param url
     * @param username
     * @param password
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String driver, String url, String username, String password) throws SQLException {
        forName(driver);
        Connection dbConn;
        DriverManager.setLoginTimeout(10);
        if (StringUtils.isEmpty(username)) {
            dbConn = DriverManager.getConnection(url);
        } else {
            dbConn = DriverManager.getConnection(url, username, password);
        }
        return dbConn;
    }

    /**
     * 关闭数据库连接
     *
     * @param conn
     * @throws SQLException
     */
    public static void close(Connection conn) throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * 关闭数据库连接 报错时不抛出异常
     *
     * @param conn
     */
    public static void closeQuietly(Connection conn) {
        try {
            close(conn);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * 关闭查询结果集
     *
     * @param rs
     * @throws SQLException
     */
    public static void close(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    public static void closeQuietly(ResultSet rs) {
        try {
            close(rs);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * 关闭 Statement
     *
     * @param stmt
     * @throws SQLException
     */
    public static void close(Statement stmt) throws SQLException {
        if (stmt != null) {
            stmt.close();
        }
    }

    public static void closeQuietly(Statement stmt) {
        try {
            close(stmt);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    public static void closeQuietly(Connection conn, Statement stmt, ResultSet rs) {
        try {
            closeQuietly(rs);
        } finally {
            closeQuietly(conn, stmt);
        }
    }

    public static void closeQuietly(Connection conn, Statement stmt) {
        try {
            closeQuietly(stmt);
        } finally {
            closeQuietly(conn);
        }
    }

    public static void close(Connection conn, Statement stmt) throws SQLException{
        close(stmt);
        close(conn);
    }


    public static void commitAndClose(Connection conn) throws SQLException {
        if (conn != null) {
            try {
                conn.commit();
            } finally {
                conn.close();
            }
        }
    }

    public static void commitAndCloseQuietly(Connection conn) {
        try {
            commitAndClose(conn);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    public static void rollback(Connection conn) throws SQLException {
        if (conn != null) {
            conn.rollback();
        }
    }

    public static void rollbackAndClose(Connection conn) throws SQLException {
        if (conn != null) {
            try {
                conn.rollback();
            } finally {
                conn.close();
            }
        }
    }

    public static void rollbackAndCloseQuietly(Connection conn) {
        try {
            rollbackAndClose(conn);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }
}