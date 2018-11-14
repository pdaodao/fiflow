package com.github.myetl.flow.core.parser.core;


import com.github.myetl.flow.core.enums.FieldType;
import com.github.myetl.flow.core.exception.SqlParseException;
import com.github.myetl.flow.core.parser.DDL;
import com.github.myetl.flow.core.parser.SQL;
import com.github.myetl.flow.core.parser.SqlTree;
import com.github.myetl.flow.core.util.SqlParseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析  create table  数据表定义信息
 */
public class DDLParser implements IParser {

    private static final String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    private static Pattern PrimaryKeyPattern = Pattern.compile("(?i)PRIMARY\\s+KEY\\s*\\((.*)\\)");


    public static DDLParser newInstance() {
        return new DDLParser();
    }


    @Override
    public boolean accept(String sql) {
        return PATTERN.matcher(sql).find();
    }

    /**
     * 解析 字段 和 类型信息
     *
     * @param fieldsStr
     * @param ddlTableInfo
     */
    private void parseFields(String fieldsStr, DDL ddlTableInfo) throws SqlParseException {
        if (StringUtils.isEmpty(fieldsStr)) return;
        String[] fields = SqlParseUtil.splitIgnoreQuotaBrackets(fieldsStr, ",");
        for (String field : fields) {
            if (StringUtils.isBlank(field)) continue;
            field = field.replaceAll("`", "").trim();
            if (processPrimaryKey(field, ddlTableInfo)) continue;
            String[] fieldAndType = field.split("\\s+");
            if (fieldAndType == null || fieldAndType.length != 2) {
                throw new SqlParseException(String.format("table [%s] field [%s] ddl error", ddlTableInfo.getTableName(), field));
            }
            FieldType fieldType = null;
            try {
                fieldType = FieldType.from(fieldAndType[1]);
            } catch (Exception e) {

            }
            if (fieldType == null) {
                throw new SqlParseException(String.format("table [%s] field [%s] unsupport field type [%s] ddl error", ddlTableInfo.getTableName(), fieldAndType[0], fieldAndType[1]));
            }
            ddlTableInfo.addField(fieldAndType[0], fieldType);
        }
    }

    /**
     * 处理主键信息  primary key(id)
     *
     * @param field
     * @param ddlTableInfo
     * @return
     */
    private boolean processPrimaryKey(String field, DDL ddlTableInfo) {
        Matcher matcher = PrimaryKeyPattern.matcher(field);
        if (matcher.find()) {
            String pks = matcher.group(1);
            String[] pkArray = pks.split(",");
            ddlTableInfo.setPrimaryKeys(Lists.newArrayList(pkArray));
            return true;
        }
        return false;
    }

    /**
     * 解析 with 块 中的属性信息
     *
     * @param withStr
     * @param ddlTableInfo
     */
    private void parseWithProps(String withStr, DDL ddlTableInfo) throws SqlParseException {
        List<String> props = SqlParseUtil.splitIgnoreQuota(withStr.trim(), ',');

        Map<String, Object> propMap = Maps.newHashMap();
        for (String prop : props) {
            List<String> line = SqlParseUtil.splitIgnoreQuota(prop, '=');
            if (line.size() != 2) {
                throw new SqlParseException(String.format("table [%s] ddl with format [%s]  error", ddlTableInfo.getTableName(), prop));
            }
            String key = line.get(0).trim();
            String value = line.get(1).trim().replaceAll("'", "").trim();
            if (key.equalsIgnoreCase("type")) {
                ddlTableInfo.setType(value);
            } else {
                propMap.put(key, value);
            }
        }
        ddlTableInfo.setProps(propMap);
    }


    @Override
    public SQL parse(String sql, SqlTree sqlTree) throws SqlParseException {
        Matcher matcher = PATTERN.matcher(sql);

        if (matcher.find()) {
            String tableName = matcher.group(1);

            DDL ddlTableInfo = new DDL(tableName);

            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);

            parseFields(fieldsInfoStr, ddlTableInfo);

            parseWithProps(propsStr, ddlTableInfo);

            for (DDL info : sqlTree.getDdls()) {
                if (tableName.equalsIgnoreCase(info.getTableName())) {
                    throw new SqlParseException(String.format("duplicated table name [%s]", tableName));
                }
            }
            sqlTree.addDDL(ddlTableInfo);
            return ddlTableInfo;
        }
        return null;
    }
}
