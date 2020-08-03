package com.github.lessonone.fiflow.common.base;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SqlSelect {
    private final String sql;
    private final Object[] args;

    private SqlSelect(String sql, Object[] args) {
        this.sql = sql;
        this.args = args;
    }

    public static SqlSelectBuilder builder() {
        return new SqlSelectBuilder();
    }

    public String getSql() {
        return sql;
    }

    public Object[] getArgs() {
        return args;
    }

    public static class SqlSelectBuilder {
        private final StringBuilder sql = new StringBuilder();
        private final List<Object> params = new ArrayList<>();
        private final StringBuilder where = new StringBuilder();

        public SqlSelect build() {
            if (where.length() > 0) {
                sql.append(" WHERE ").append(where.toString());
            }
            return new SqlSelect(sql.toString(), params.stream().toArray());
        }

        public SqlSelectBuilder count(Class<?> clazz) {
            sql.append("SELECT count(1) FROM ").append(DaoUtils.getTableName(clazz));
            return this;
        }

        public SqlSelectBuilder select(String... fields) {
            if (fields == null) return this;
            sql.append("SELECT ");
            sql.append(StringUtils.join(fields, ','));
            return this;
        }

        public SqlSelectBuilder delete(Class<?> clazz) {
            sql.append("DELETE FROM ").append(DaoUtils.getTableName(clazz));
            return this;
        }

        public SqlSelectBuilder from(Class<?> clazz, @Nullable String tableAlias) {
            sql.append(" FROM ").append(DaoUtils.getTableName(clazz));
            if (tableAlias != null)
                sql.append(" ").append(tableAlias);
            return this;
        }

        public SqlSelectBuilder from(Class<?> clazz) {
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

        private SqlSelectBuilder joinOn(String on) {
            if (on == null) return this;
            sql.append(" ON ").append(on);
            return this;
        }

        public Where where(String field) {
            return and(field);
        }

        public SqlSelectBuilder whereSql(String sqlFragment, Object... args) {
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
        final SqlSelectBuilder sqlSelect;
        final String field;
        final String logical;

        public Where(SqlSelectBuilder sqlSelect, String field, String logical) {
            this.sqlSelect = sqlSelect;
            this.field = field;
            this.logical = logical;
        }

        private void before() {
            if (sqlSelect.where.length() > 1) {
                sqlSelect.where.append(logical);
            }
        }

        public SqlSelectBuilder equal(Object value) {
            before();
            sqlSelect.where.append(field).append(" = ?");
            return addArgs(value);
        }

        public SqlSelectBuilder in(SqlSelect inSelect) {
            before();
            sqlSelect.where.append(field).append(" IN ");
            sqlSelect.where.append("(")
                    .append(StringUtils.repeat("?", ",", inSelect.getArgs().length))
                    .append(")");
            return addArgs(inSelect.getArgs());
        }

        public SqlSelectBuilder sqlArgs(Object... args) {
            before();
            sqlSelect.where.append(field);
            return addArgs(args);
        }

        public SqlSelectBuilder addArgs(Object... args) {
            if (args != null) {
                for (Object arg : args) {
                    sqlSelect.params.add(arg);
                }
            }
            return sqlSelect;
        }


        public SqlSelectBuilder equalIgnoreNull(Object value) {
            if (value == null) return sqlSelect;
            before();
            sqlSelect.where.append(field).append(" = ?");
            sqlSelect.params.add(value);
            return sqlSelect;
        }
    }

    public static class Join {
        final SqlSelectBuilder sqlSelect;

        public Join(SqlSelectBuilder sqlSelect) {
            this.sqlSelect = sqlSelect;
        }

        public SqlSelectBuilder on(String on) {
            return sqlSelect.joinOn(on);
        }
    }
}
