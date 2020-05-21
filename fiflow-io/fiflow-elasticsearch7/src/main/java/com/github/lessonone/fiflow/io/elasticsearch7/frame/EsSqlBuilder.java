package com.github.lessonone.fiflow.io.elasticsearch7.frame;

import com.github.lessonone.fiflow.core.util.SqlParserUtils;
import org.apache.calcite.sql.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class EsSqlBuilder {

    /**
     * 使用 sql 构建查询请求
     *
     * @param sql
     * @return
     * @throws Exception
     */
    public static Tuple2<SearchRequest, List<String>> build(String sql) throws Exception {
        SqlNode sqlNode = SqlParserUtils.parse(sql);

        List<String> selectFields = new ArrayList<>();
        String from = null;
        QueryBuilder where = null;
        if (sqlNode instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) sqlNode;
            List<SqlNode> fields = select.getSelectList().getList();
            for (SqlNode f : fields) {
                selectFields.add(f.toString());
            }
            SqlNode fromNode = select.getFrom();
            from = fromNode.toString();
            where = buildWhere(select.getWhere());
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if (where == null) {
            where = QueryBuilders.matchAllQuery();
        }

        searchSourceBuilder.query(where);
        searchSourceBuilder.fetchSource(selectFields.toArray(new String[0]), null);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(from);
        searchRequest.source(searchSourceBuilder);
        return new Tuple2<>(searchRequest, selectFields);
    }

    /**
     * 把 sql 的 查询条件 转为 elasticsearch 的查询语言
     *
     * @param where
     * @return
     */
    private static QueryBuilder buildWhere(SqlNode where) {
        if (where == null || !(where instanceof SqlBasicCall)) {
            return null;
        }
        SqlBasicCall wh = (SqlBasicCall) where;

        SqlKind kind = wh.getOperator().kind;

        if (kind == SqlKind.AND || kind == SqlKind.OR) {
            List<QueryBuilder> subList = new ArrayList<>();
            for (SqlNode children : wh.operands) {
                QueryBuilder sub = buildWhere(children);
                if (sub != null) {
                    subList.add(sub);
                }
            }
            if (subList.size() == 0)
                return null;
            if (subList.size() == 1)
                return subList.get(0);

            BoolQueryBuilder top = QueryBuilders.boolQuery();
            subList.forEach(t -> {
                if (kind == SqlKind.AND) {
                    top.must(t);
                } else {
                    top.should(t);
                }
            });
            return top;
        }
        String field = getField(wh);
        Object v = getValue(wh);

        switch (kind) {
            case EQUALS:
                return QueryBuilders.termQuery(field, v);
            case GREATER_THAN:
                return QueryBuilders.rangeQuery(field).gt(v);
            case GREATER_THAN_OR_EQUAL:
                return QueryBuilders.rangeQuery(field).gte(v);
            case LESS_THAN:
                return QueryBuilders.rangeQuery(field).lt(v);
            case LESS_THAN_OR_EQUAL:
                return QueryBuilders.rangeQuery(field).lte(v);
            case LIKE:
                return QueryBuilders.matchQuery(field, v);
            case IN:
                return QueryBuilders.termsQuery(field, (Collection) v);
        }

        return null;
    }

    public static String getField(SqlBasicCall wh) {
        return wh.operands[0].toString();
    }

    public static Object getValue(SqlBasicCall wh) {
        if (wh.operands == null || wh.operands.length < 2)
            return null;
        SqlNode node = wh.operands[1];

        return parseValue(node);
    }

    public static Object parseValue(SqlNode sqlNode) {
        if (sqlNode == null) return null;
        if (sqlNode instanceof SqlLiteral) {
            return ((SqlLiteral) sqlNode).getValue();
        }
        if (sqlNode instanceof SqlIdentifier) {
            return sqlNode.toString();
        }

        if (sqlNode instanceof SqlNodeList) {
            return ((SqlNodeList) sqlNode).getList().stream()
                    .map(t -> parseValue(t)).collect(Collectors.toList());
        }

        return sqlNode.toString();
    }


}
