package com.github.lessonone.fiflow.common.base;

import com.github.lessonone.fiflow.common.entity.BaseEntity;
import com.github.lessonone.fiflow.common.utils.DbUtils;
import com.github.lessonone.fiflow.common.utils.JSON;
import com.github.lessonone.fiflow.common.utils.StrUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.beans.PropertyDescriptor;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;

public class BaseDao {
    public static final Logger logger = LoggerFactory.getLogger(BaseDao.class);
    public static final String PkColumn = "id";
    private transient final DataSource dataSource;
    private transient final JdbcTemplate jdbcTemplate;
    private transient final TransactionTemplate transactionTemplate;

    public BaseDao(DataSource ds) {
        this.dataSource = ds;
        this.jdbcTemplate = DbUtils.createJdbcTemplate(ds);
        this.transactionTemplate = DbUtils.createTransactionTemplate(ds);
    }

    private static Object processUpdateSqlValue(Object val) {
        if (val == null) return null;
        if (val instanceof Map || val instanceof Collection) {
            try {
                return JSON.toString(val);
            } catch (JsonProcessingException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return val;
    }

    public <T> List<T> queryForList(String sql, Class<T> elementType, @Nullable Object... args) {
        if (BeanUtils.isSimpleValueType(elementType)) {
            return jdbcTemplate.queryForList(sql, elementType, args);
        }
        return jdbcTemplate.query(sql, new MyRowMapper<>(elementType), args);
    }

    public <T> Optional<T> queryForOne(String sql, Class<T> elementType, @Nullable Object... args) {
        List<T> t = queryForList(sql, elementType, args);
        if (t == null || t.size() == 0) return Optional.empty();
        if (t.size() > 1) throw new RuntimeException("expect one result but found " + t.size());
        return Optional.of(t.get(0));
    }

    protected void transactionWrap(final Supplier f) {
        transactionTemplate.execute(new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(TransactionStatus transactionStatus) {
                return f.get();
            }
        });
    }

    public <T extends BaseEntity> Long insertSelective(final String tableName, T entity) {
        Tuple2<Long, Map<String, Object>> t = entityToMap(entity);
        if (t == null) throw new RuntimeException("insert entity is null");
        return insertInto(tableName, t.f1, true);
    }

    public <T extends BaseEntity> int updateSelective(final String tableName, T entity) {
        Tuple2<Long, Map<String, Object>> t = entityToMap(entity);
        if (t == null) return 0;
        if (t.f0 == null) throw new RuntimeException("updateSelective id is null");
        return updateById(tableName, t.f0, t.f1, true);
    }

    protected <T extends BaseEntity> Tuple2<Long, Map<String, Object>> entityToMap(T entity) {
        if (entity == null) return null;
        PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(entity.getClass());
        Map<String, Object> rowMap = new LinkedHashMap<>();
        Long id = null;
        for (PropertyDescriptor pd : pds) {
            String column = StrUtil.toUnderlineCase(pd.getName());
            Object value = pd.getValue(pd.getName());
            if (value == null)
                continue;
            if (PkColumn.equals(column)) {
                id = (Long) value;
                continue;
            }
            rowMap.put(column, value);
        }
        return new Tuple2<Long, Map<String, Object>>(id, rowMap);
    }

    public Long insertInto(final String tableName, Map<String, Object> rowMap, boolean isIgnoreNull) {
        StringBuilder insert = new StringBuilder("INSERT INTO ").append(tableName).append("(");
        List<String> fields = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        for (Map.Entry<String, Object> entry : rowMap.entrySet()) {
            if (entry.getValue() == null && isIgnoreNull) continue;
            fields.add(entry.getKey());
            params.add(processUpdateSqlValue(entry.getValue()));
        }
        insert.append(StringUtils.join(fields, ","));
        insert.append(") ");
        insert.append(" VALUES (");
        insert.append(StringUtils.repeat("?", ",", fields.size()));
        insert.append(")");
        return insertReturnAutoId(insert.toString(), params.stream().toArray());
    }

    public int update(String sql, Object... args) {
        return jdbcTemplate.update(sql, args);
    }

    public int updateById(String tableName, Long id, Map<String, Object> rowMap, boolean isIgnoreNull) {
        if (rowMap == null) return 0;
        StringBuilder sql = new StringBuilder("UPDATE " + tableName + " SET ");
        List<String> fields = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        for (Map.Entry<String, Object> entry : rowMap.entrySet()) {
            if (entry.getValue() == null && isIgnoreNull) continue;
            fields.add(entry.getKey() + "=?");
            params.add(processUpdateSqlValue(entry.getValue()));
        }
        if (CollectionUtils.isEmpty(fields)) return 0;
        sql.append(StringUtils.join(fields, ","));
        sql.append(" WHERE ").append(PkColumn).append(" = ?");
        params.add(id);

        return jdbcTemplate.update(sql.toString(), params.stream().toArray());
    }

    public Long insertReturnAutoId(final String sql, final Object... args) {
        final KeyHolder keyHolder = new GeneratedKeyHolder();
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

    public static class MyRowMapper<T> extends BeanPropertyRowMapper<T> {
        public MyRowMapper(Class<T> mappedClass) {
            super(mappedClass);
        }

        @Override
        protected Object getColumnValue(ResultSet rs, int index, PropertyDescriptor pd) throws SQLException {
            Class<?> pp = pd.getPropertyType();
            boolean needJson = false;
            if (pp == List.class || pp == Map.class) {
                pp = String.class;
                needJson = true;
            }
            Object v = JdbcUtils.getResultSetValue(rs, index, pp);
            if (v != null && needJson) {
                try {
                    v = JSON.toPojo((String) v, pd.getPropertyType());
                } catch (Exception e) {
                    throw new SQLException(e.getMessage(), e);
                }
            }
            return v;
        }
    }
}
