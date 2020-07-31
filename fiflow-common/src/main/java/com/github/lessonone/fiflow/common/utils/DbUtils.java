package com.github.lessonone.fiflow.common.utils;

import com.github.lessonone.fiflow.common.base.DbInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DbUtils {
    private static final Map<String, HikariDataSource> sourceMap = new ConcurrentHashMap<>();

    public static Map<String, String> getValueAsMapString(Map<String, Object> row, String key) {
        if (row == null) return null;
        Object v = row.get(key);
        if (v == null) return new HashMap<>();
        try {
            return JSON.toPojo(v.toString(), Map.class);
        } catch (JsonProcessingException e) {

        }
        return new HashMap<>();
    }

    public static String getValueAsString(Map<String, Object> row, String key) {
        if (row == null) return null;
        Object v = row.get(key);
        if (v == null) return null;
        return v.toString();
    }

    public static Long getValueAsLong(Map<String, Object> row, String key) {
        if (row == null) return null;
        Object v = row.get(key);
        if (v == null) return 0l;
        if (v instanceof Long || v instanceof Integer) return (Long) v;
        return Long.parseLong(v.toString());
    }

    public static JdbcTemplate createJdbcTemplate(DbInfo dbInfo) {
        DataSource dataSource = createDatasource(dbInfo);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate;
    }

    public static JdbcTemplate createJdbcTemplate(DataSource ds){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        return jdbcTemplate;
    }

    public static TransactionTemplate createTransactionTemplate(DataSource ds) {
        DataSourceTransactionManager manager = new DataSourceTransactionManager(ds);
        TransactionTemplate template = new TransactionTemplate(manager);
        return template;
    }

    public static void closeDatasource(DbInfo dbInfo) {
        final String key = getKey(dbInfo);
        if (sourceMap.containsKey(key)) {
            sourceMap.get(key).close();
            sourceMap.remove(key);
        }
    }

    public static void print(ResultSet rs) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            for (int i = 1; i <= count; i++) {
                System.out.print(meta.getColumnName(i));
                System.out.print("  |   ");
            }
            System.out.println();
            System.out.println("----------------------------");
            System.out.println();

            while (rs.next()) {
                for (int i = 1; i <= count; i++) {
                    System.out.print(rs.getString(i));
                    System.out.print("  |   ");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static String getKey(DbInfo dbInfo) {
        return new StringBuilder(dbInfo.getUrl())
                .append(dbInfo.getUsername())
                .append(dbInfo.getPassword())
                .toString();
    }

    public static DataSource createDatasource(DbInfo dbInfo) {
        final String key = getKey(dbInfo);
        if (sourceMap.containsKey(key)) {
            if (sourceMap.get(key).isClosed()) {
                sourceMap.remove(key);
            }
        }

        if (!sourceMap.containsKey(key)) {
            synchronized (sourceMap) {
                if (!sourceMap.containsKey(key)) {
                    HikariDataSource dataSource = new HikariDataSource();
                    dataSource.setJdbcUrl(dbInfo.getUrl());
                    dataSource.setUsername(dbInfo.getUsername());
                    dataSource.setPassword(dbInfo.getPassword());
                    if (dbInfo.getDriverClassName() != null)
                        dataSource.setDriverClassName(dbInfo.getDriverClassName());
                    sourceMap.put(key, dataSource);
                }
            }
        }

        return sourceMap.get(key);
    }
}
