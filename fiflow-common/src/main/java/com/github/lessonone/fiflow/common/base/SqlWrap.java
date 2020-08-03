package com.github.lessonone.fiflow.common.base;

import com.github.lessonone.fiflow.common.utils.DaoUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SqlWrap {
    private final String sql;
    private final Object[] args;

    private SqlWrap(String sql, Object[] args) {
        this.sql = sql;
        this.args = args;
    }

    public static SqlWrapBuilder builder() {
        return new SqlWrapBuilder();
    }

    public String getSql() {
        return sql;
    }

    public Object[] getArgs() {
        return args;
    }

    public static class SqlWrapBuilder {
        private final StringBuilder sql = new StringBuilder();
        private final List<Object> params = new ArrayList<>();
        private final StringBuilder where = new StringBuilder();

        public SqlWrap build() {
            if (where.length() > 0) {
                sql.append(" WHERE ").append(where.toString());
            }
            return new SqlWrap(sql.toString(), params.stream().toArray());
        }

        public SqlWrapBuilder count(Class<?> clazz) {
            sql.append("SELECT count(1) FROM ").append(DaoUtils.getTableName(clazz));
            return this;
        }

        public SqlWrapBuilder select(String... fields) {
            if (fields == null) return this;
            sql.append("SELECT ");
            sql.append(StringUtils.join(fields, ','));
            return this;
        }

        public SqlWrapBuilder delete(Class<?> clazz) {
            sql.append("DELETE FROM ").append(DaoUtils.getTableName(clazz));
            return this;
        }

        public SqlWrapBuilder from(Class<?> clazz, @Nullable String tableAlias) {
            sql.append(" FROM ").append(DaoUtils.getTableName(clazz));
            if (tableAlias != null)
                sql.append(" ").append(tableAlias);
            return this;
        }

        public SqlWrapBuilder from(Class<?> clazz) {
            return from(clazz, null);
        }

        public Join leftJoin(Class<?> clazz, String tableAlias) {
            sql.append(" LEFT JOIN ").append(DaoUtils.getTableName(clazz)).append(" ").append(tableAlias);
            return new Join(this);
        }

        public Join innerJoin(Class<?> clazz, String tableAlias) {
            sql.append(" INNER JOIN ").append(DaoUtils.getTableName(clazz)).append(" ").append(tableAlias);
            return new Join(this);
        }

        public Join rightJoin(Class<?> clazz, String tableAlias) {
            sql.append(" RIGHT JOIN ").append(DaoUtils.getTableName(clazz)).append(" ").append(tableAlias);
            return new Join(this);
        }

        private SqlWrapBuilder joinOn(String on) {
            if (on == null) return this;
            sql.append(" ON ").append(on);
            return this;
        }

        public Where where(String field) {
            return and(field);
        }

        public SqlWrapBuilder whereSql(String sqlFragment, Object... args) {
            return and(sqlFragment).sqlArgs(args);
        }

        public Where and(String field) {
            return where(field, " AND ");
        }

        public Where or(String field) {
            return where(field, " OR ");
        }

        public Where where(String field, String logical) {
            if (StringUtils.isEmpty(field)) throw new IllegalArgumentException("where field is empty");
            return new Where(this, field, logical);
        }
    }

    public static class Where {
        final SqlWrapBuilder sqlSelect;
        final String field;
        final String logical;

        public Where(SqlWrapBuilder sqlSelect, String field, String logical) {
            this.sqlSelect = sqlSelect;
            this.field = field;
            this.logical = logical;
        }

        private void before() {
            if (sqlSelect.where.length() > 1) {
                sqlSelect.where.append(logical);
            }
        }

        public SqlWrapBuilder equal(Object value) {
            before();
            sqlSelect.where.append(field).append(" = ?");
            return addArgs(value);
        }

        public SqlWrapBuilder in(SqlWrap inSelect) {
            before();
            sqlSelect.where.append(field).append(" IN ");
            sqlSelect.where.append("(")
                    .append(StringUtils.repeat("?", ",", inSelect.getArgs().length))
                    .append(")");
            return addArgs(inSelect.getArgs());
        }

        public SqlWrapBuilder sqlArgs(Object... args) {
            before();
            sqlSelect.where.append(field);
            return addArgs(args);
        }

        public SqlWrapBuilder addArgs(Object... args) {
            if (args != null) {
                for (Object arg : args) {
                    sqlSelect.params.add(arg);
                }
            }
            return sqlSelect;
        }


        public SqlWrapBuilder equalIgnoreNull(Object value) {
            if (value == null) return sqlSelect;
            before();
            sqlSelect.where.append(field).append(" = ?");
            sqlSelect.params.add(value);
            return sqlSelect;
        }
    }

    public static class Join {
        final SqlWrapBuilder sqlSelect;

        public Join(SqlWrapBuilder sqlSelect) {
            this.sqlSelect = sqlSelect;
        }

        public SqlWrapBuilder on(String on) {
            return sqlSelect.joinOn(on);
        }
    }
}
