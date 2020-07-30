package com.github.lessonone.fiflow.common.utils;

import com.github.lessonone.fiflow.common.base.DbInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DbUtils {
    private static final Map<String, HikariDataSource> sourceMap = new ConcurrentHashMap<>();
    private static final Map<String, TransactionTemplate> transactionMap = new ConcurrentHashMap<>();

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

    public static TransactionTemplate createTransactionTemplate(DbInfo dbInfo) {
        final String key = getKey(dbInfo);
        if (transactionMap.containsKey(key)) return transactionMap.get(key);
        DataSource dataSource = createDatasource(dbInfo);
        DataSourceTransactionManager manager = new DataSourceTransactionManager(dataSource);
        TransactionTemplate template = new TransactionTemplate(manager);
        transactionMap.put(key, template);
        return template;
    }

    public static void closeDatasource(DbInfo dbInfo) {
        final String key = getKey(dbInfo);
        if (sourceMap.containsKey(key)) {
            sourceMap.get(key).close();
            sourceMap.remove(key);
        }
    }

    /**
     * insert into 忽略值为 null 的字段
     *
     * @param jdbcTemplate
     * @param tableName
     * @param rowMap
     * @return
     */
    public static int insertIntoIgnoreNull(JdbcTemplate jdbcTemplate, String tableName, Map<String, Object> rowMap) {
        Tuple2<String, Object[]> insert = generateInsertSqlIgnoreNull(tableName, rowMap);
        if (insert == null) return 0;
        return jdbcTemplate.update(insert.f0, insert.f1);
    }

    public static int updateIgnoreNullById(JdbcTemplate jdbcTemplate, String tableName, Map<String, Object> rowMap, Long id) {
        if (rowMap == null) return 0;
        String sql = "UPDATE " + tableName + " SET ";
        List<String> fields = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        for (Map.Entry<String, Object> entry : rowMap.entrySet()) {
            if (entry.getValue() == null) continue;
            fields.add(entry.getKey() + "=?");
            params.add(processUpdateSqlValue(entry.getValue()));
        }
        if (CollectionUtils.isEmpty(fields)) return 0;
        sql += StringUtils.join(fields, ",");
        return jdbcTemplate.update(sql, params.stream().toArray());
    }

    private static Object processUpdateSqlValue(Object val) {
        if (val == null) return null;
        if (val instanceof Map || val instanceof Collection) {
            try {
                return JSON.toString(val);
            } catch (JsonProcessingException e) {
                throw new CatalogException(e);
            }
        }
        return val;
    }

    public static Tuple2<String, Object[]> generateInsertSqlIgnoreNull(String table, Map<String, Object> rowMap) {
        if (StringUtils.isEmpty(table) || rowMap == null) return null;
        StringBuilder insert = new StringBuilder("INSERT INTO ").append(table).append("(");
        List<String> fields = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        for (Map.Entry<String, Object> entry : rowMap.entrySet()) {
            if (entry.getValue() == null) continue;
            fields.add(entry.getKey());
            params.add(processUpdateSqlValue(entry.getValue()));
        }
        insert.append(StringUtils.join(fields, ","));
        insert.append(") ");
        insert.append(" VALUES (");
        insert.append(StringUtils.repeat("?", ",", fields.size()));
        insert.append(")");
        return new Tuple2<>(insert.toString(), params.stream().toArray());
    }


    /**
     * insert 返回 自增主键
     *
     * @param jdbcTemplate
     * @param tableName
     * @param rowMap
     * @return
     */
    public static Long insertReturnAutoId(JdbcTemplate jdbcTemplate, String tableName, Map<String, Object> rowMap) {
        Tuple2<String, Object[]> insert = generateInsertSqlIgnoreNull(tableName, rowMap);
        if (insert == null) return null;
        return insertReturnAutoId(jdbcTemplate, insert.f0, insert.f1);
    }

    /**
     * insert 返回 自增主键
     *
     * @param jdbcTemplate
     * @param sql
     * @param args
     * @return
     */
    public static Long insertReturnAutoId(JdbcTemplate jdbcTemplate, final String sql, final Object... args) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                int index = 1;
                for (Object arg : args) {
                    ps.setObject(index++, arg);
                }
                return ps;
            }
        }, keyHolder);
        return keyHolder.getKey().longValue();
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
